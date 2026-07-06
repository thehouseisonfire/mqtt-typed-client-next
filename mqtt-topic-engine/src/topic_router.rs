//! Subscription router built on the topic matcher.
//!
//! [`TopicRouter`] maps MQTT subscription patterns to caller-supplied payloads
//! `T`, assigns each a [`SubscriptionId`], and resolves all payloads whose
//! pattern matches a delivered topic. It also tracks the set of distinct broker
//! subscriptions (with their effective `QoS`) so callers know when to actually
//! subscribe or unsubscribe on the wire.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use arcstr::ArcStr;
use thiserror::Error;

use crate::qos::QoS;
use crate::topic_match::TopicPath;
use crate::topic_matcher::{TopicMatcherError, TopicMatcherNode};
use crate::topic_pattern_item::TopicPatternError;
use crate::topic_pattern_path::TopicPatternPath;

/// Errors that can occur during topic routing operations
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TopicRouterError {
    /// Topic pattern validation failed
    #[error("Invalid topic pattern: {0}")]
    InvalidPattern(#[from] TopicPatternError),

    /// Topic matching operation failed
    #[error("Topic matching failed: {0}")]
    MatchingFailed(#[from] TopicMatcherError),

    /// Subscription with given ID was not found
    #[error("Subscription {id:?} not found")]
    SubscriptionNotFound {
        /// The subscription id that could not be found.
        id: SubscriptionId,
    },

    /// Topic is invalid for routing operations
    #[error("Topic '{topic}' is invalid for routing: {reason}")]
    InvalidRoutingTopic {
        /// The topic that was rejected.
        topic: String,
        /// Why the topic is invalid for routing.
        reason: String,
    },

    /// Internal state corruption detected
    #[error("Internal routing state corrupted: {details}")]
    InternalStateCorrupted {
        /// Description of the detected inconsistency.
        details: String,
    },
}

impl TopicRouterError {
    /// Creates a new `SubscriptionNotFound` error
    #[must_use]
    pub const fn subscription_not_found(id: SubscriptionId) -> Self {
        Self::SubscriptionNotFound { id }
    }

    /// Creates a new `InvalidRoutingTopic` error
    pub fn invalid_routing_topic(topic: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidRoutingTopic {
            topic: topic.into(),
            reason: reason.into(),
        }
    }

    /// Creates a new `InternalStateCorrupted` error
    pub fn internal_state_corrupted(details: impl Into<String>) -> Self {
        Self::InternalStateCorrupted {
            details: details.into(),
        }
    }
}

/// A subscription identifier.
///
/// Used for tracking individual subscriptions and handling cancellation errors.
/// Primarily useful for advanced error handling and debugging.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct SubscriptionId(usize);

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubscriptionId({})", self.0)
    }
}

/// Broker action needed after removing a local subscription.
#[derive(Debug, Clone)]
pub enum UnsubscribeAction {
    /// The effective broker subscription did not change.
    NoBrokerAction {
        /// The topic pattern that still has local subscribers.
        topic: TopicPatternPath,
    },

    /// No local subscribers remain for this topic pattern.
    Unsubscribe {
        /// The topic pattern to unsubscribe from on the broker.
        topic: TopicPatternPath,
    },

    /// Local subscribers remain, but their maximum requested `QoS` is lower.
    Resubscribe {
        /// The topic pattern to resubscribe to on the broker.
        topic: TopicPatternPath,
        /// The new maximum `QoS` requested by remaining local subscribers.
        qos: QoS,
    },
}

type SubscriptionTable<T> = HashMap<SubscriptionId, T>;
//type RouteCallback = Box<dyn for<'a, 'b> Fn(&'a str, &'b [u8]) + Send + Sync>;

/// Routes MQTT topics to subscription payloads.
///
/// Each subscription pattern is associated with a payload `T` and a unique
/// [`SubscriptionId`]. Delivered topics are matched against all stored patterns
/// (including `+`/`#` wildcards); the router also bookkeeps which distinct
/// broker subscriptions are active and at what `QoS`.
pub struct TopicRouter<T> {
    topic_matcher: TopicMatcherNode<SubscriptionTable<T>>,
    subscriptions: SubscriptionTable<(TopicPatternPath, QoS)>,
    next_id: usize,
}

impl<T> Default for TopicRouter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TopicRouter<T> {
    /// Creates an empty router with no subscriptions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            topic_matcher: TopicMatcherNode::new(),
            subscriptions: SubscriptionTable::new(),
            next_id: 0,
        }
    }

    /// Registers a subscription for `topic` with the given `qos` and payload.
    ///
    /// Returns `(needs_subscribe, id)`: `needs_subscribe` is `true` when this is
    /// the first subscription for the pattern or it raises the effective `QoS`, so
    /// the caller must (re)subscribe on the broker; `id` identifies the new
    /// subscription for later [`unsubscribe`](Self::unsubscribe).
    pub fn add_subscription(
        &mut self,
        topic: TopicPatternPath,
        qos: impl Into<QoS>,
        subscription: T,
    ) -> (bool, SubscriptionId) {
        let qos = qos.into();
        let subscription_table = self.topic_matcher.get_or_create_subscription_table(&topic);
        let needs_subscribe = subscription_table
            .keys()
            .map(|id| {
                self.subscriptions
                    .get(id)
                    .unwrap_or_else(|| {
                        panic!(
                            "BUG: Subscription ID {id:?} exists in topic \
							 matcher but missing from subscriptions. Topic: \
							 {topic}"
                        )
                    })
                    .1
            })
            .max_by_key(|qos| *qos as u8)
            .is_none_or(|max| qos > max);

        let id = SubscriptionId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);

        subscription_table.insert(id, subscription);
        self.subscriptions.insert(id, (topic, qos));

        (needs_subscribe, id)
    }

    /// Removes the subscription identified by `id`.
    ///
    /// Returns the broker action required after the removal. Fails with
    /// [`TopicRouterError::SubscriptionNotFound`] if `id` is unknown.
    pub fn unsubscribe(
        &mut self,
        id: &SubscriptionId,
    ) -> Result<UnsubscribeAction, TopicRouterError> {
        let topic = self.subscriptions.remove(id);
        match topic {
            Some((topic_pattern, removed_qos)) => {
                let resolved_segments = topic_pattern.resolve_bound_segments();
                let subscriptions = &self.subscriptions;
                let mut pattern_now_empty = false;
                let mut remaining_qos = None;
                self.topic_matcher
                    .update_node(&resolved_segments, |table| {
                        table.remove(id);
                        pattern_now_empty = table.is_empty();
                        if !pattern_now_empty {
                            remaining_qos = Some(Self::get_max_qos_for_topic(
                                subscriptions,
                                &topic_pattern,
                                table,
                            ));
                        }
                    })?;
                if pattern_now_empty {
                    Ok(UnsubscribeAction::Unsubscribe {
                        topic: topic_pattern,
                    })
                } else if let Some(qos) = remaining_qos {
                    if qos < removed_qos {
                        Ok(UnsubscribeAction::Resubscribe {
                            topic: topic_pattern,
                            qos,
                        })
                    } else {
                        Ok(UnsubscribeAction::NoBrokerAction {
                            topic: topic_pattern,
                        })
                    }
                } else {
                    Err(TopicRouterError::internal_state_corrupted(format!(
                        "Topic matcher reported non-empty state but no subscribers remained for topic {topic_pattern}"
                    )))
                }
            }
            None => Err(TopicRouterError::subscription_not_found(*id)),
        }
    }

    /// Returns every subscription whose pattern matches `topic`.
    ///
    /// Each entry is `(id, (pattern, qos), payload)` for a matching subscription
    /// (one delivered topic may match several patterns via `+`/`#` wildcards).
    #[must_use]
    pub fn get_subscribers<'a>(
        &'a self,
        topic: &TopicPath,
    ) -> Vec<(&'a SubscriptionId, &'a (TopicPatternPath, QoS), &'a T)> {
        let subscribers = self.topic_matcher.find_by_path(topic);
        subscribers
            .into_iter()
            .flat_map(|hash_map| hash_map.iter())
            .map(|(id, subscription)| {
                let topic_pattern = self
                    .subscriptions
                    .get(id)
                    .expect("Subscription ID should exist in subscriptions");
                (id, topic_pattern, subscription)
            })
            .collect()
    }

    /// Iterates over every active subscription as `(pattern, qos)`.
    ///
    /// Patterns are not deduplicated; multiple subscriptions may share one.
    pub fn get_active_subscriptions(&self) -> impl Iterator<Item = &(TopicPatternPath, QoS)> {
        self.subscriptions.values()
    }

    /// Finds the maximum `QoS` among the subscribers to a single topic pattern.
    fn get_max_qos_for_topic(
        subscriptions: &SubscriptionTable<(TopicPatternPath, QoS)>,
        topic: &TopicPatternPath,
        topic_subscriptions: &SubscriptionTable<T>,
    ) -> QoS {
        debug_assert!(
            !topic_subscriptions.is_empty(),
            "topic_subscriptions should never be empty - this is guaranteed \
			 by collect_active_subscriptions()"
        );

        topic_subscriptions
            .keys()
            .map(|id| {
                subscriptions
                    .get(id)
                    .unwrap_or_else(|| {
                        panic!(
                            "BUG: Subscription ID {id:?} exists in topic \
							 matcher but missing from subscriptions. Topic: \
							 {topic}"
                        )
                    })
                    .1
            })
            .max_by_key(|qos| *qos as u8)
            .unwrap()
    }

    /// Get all unique active topic patterns
    #[must_use]
    pub fn get_topics_for_unsubscribe(&self) -> HashSet<ArcStr> {
        self.subscriptions
            .values()
            .map(|(topic, _)| topic.mqtt_pattern())
            .collect()
    }

    /// Get all active topic patterns with their maximum `QoS`
    /// Returns unique topics (grouped by pattern) with the highest `QoS` among all subscribers
    #[must_use]
    pub fn get_topics_for_resubscribe(&self) -> HashMap<ArcStr, QoS> {
        let mut result: HashMap<ArcStr, QoS> = HashMap::new();

        for (topic, qos) in self.subscriptions.values() {
            let mqtt_pattern = topic.mqtt_pattern();
            result
                .entry(mqtt_pattern)
                .and_modify(|existing_qos| {
                    if *qos > *existing_qos {
                        *existing_qos = *qos;
                    }
                })
                .or_insert(*qos);
        }

        result
    }

    /// Cleanup all internal data structures and close subscriber channels
    /// This method is called during shutdown to ensure proper resource cleanup
    pub fn cleanup(&mut self) {
        // Replacing topic_matcher with new instance triggers Drop for all subscription channels
        // This ensures all subscribers receive a channel close signal
        self.topic_matcher = TopicMatcherNode::new();
        self.subscriptions.clear();
        self.next_id = 0;
    }

    /// Looks up the `(pattern, qos)` registered for a subscription `id`.
    ///
    /// Fails with [`TopicRouterError::SubscriptionNotFound`] if `id` is unknown.
    pub fn get_topic_by_id(
        &self,
        id: &SubscriptionId,
    ) -> Result<&(TopicPatternPath, QoS), TopicRouterError> {
        self.subscriptions
            .get(id)
            .ok_or(TopicRouterError::subscription_not_found(*id))
    }
}

#[cfg(test)]
mod tests {
    use super::{TopicRouter, TopicRouterError, UnsubscribeAction};
    use crate::{CacheStrategy, QoS, TopicPatternPath};

    fn pattern(value: &str) -> TopicPatternPath {
        TopicPatternPath::new_from_string(value, CacheStrategy::NoCache).unwrap()
    }

    fn assert_topic(topic: &TopicPatternPath, expected: &str) {
        assert_eq!(topic.mqtt_pattern().as_str(), expected);
    }

    #[test]
    fn unsubscribe_only_subscriber_requires_broker_unsubscribe() {
        let mut router = TopicRouter::new();
        let (_, id) = router.add_subscription(pattern("sensors/+/data"), QoS::AtLeastOnce, "h1");

        match router.unsubscribe(&id).unwrap() {
            UnsubscribeAction::Unsubscribe { topic } => {
                assert_topic(&topic, "sensors/+/data");
            }
            action => panic!("expected broker unsubscribe, got {action:?}"),
        }
    }

    #[test]
    fn unsubscribe_lower_qos_subscriber_keeps_broker_subscription_unchanged() {
        let mut router = TopicRouter::new();
        let (_, high_id) =
            router.add_subscription(pattern("sensors/+/data"), QoS::ExactlyOnce, "h1");
        let (_, low_id) = router.add_subscription(pattern("sensors/+/data"), QoS::AtMostOnce, "h2");

        match router.unsubscribe(&low_id).unwrap() {
            UnsubscribeAction::NoBrokerAction { topic } => {
                assert_topic(&topic, "sensors/+/data");
            }
            action => panic!("expected no broker action, got {action:?}"),
        }

        assert!(router.get_topic_by_id(&high_id).is_ok());
    }

    #[test]
    fn unsubscribe_only_high_qos_subscriber_requires_broker_downgrade() {
        let mut router = TopicRouter::new();
        let (_, low_id) = router.add_subscription(pattern("sensors/+/data"), QoS::AtMostOnce, "h1");
        let (_, mid_id) =
            router.add_subscription(pattern("sensors/+/data"), QoS::AtLeastOnce, "h2");
        let (_, high_id) =
            router.add_subscription(pattern("sensors/+/data"), QoS::ExactlyOnce, "h3");

        match router.unsubscribe(&high_id).unwrap() {
            UnsubscribeAction::Resubscribe { topic, qos } => {
                assert_topic(&topic, "sensors/+/data");
                assert_eq!(qos, QoS::AtLeastOnce);
            }
            action => panic!("expected broker resubscribe, got {action:?}"),
        }

        assert!(router.get_topic_by_id(&low_id).is_ok());
        assert!(router.get_topic_by_id(&mid_id).is_ok());
    }

    #[test]
    fn unsubscribe_one_of_multiple_high_qos_subscribers_keeps_broker_subscription_unchanged() {
        let mut router = TopicRouter::new();
        let (_, high_id_1) =
            router.add_subscription(pattern("sensors/+/data"), QoS::ExactlyOnce, "h1");
        let (_, high_id_2) =
            router.add_subscription(pattern("sensors/+/data"), QoS::ExactlyOnce, "h2");
        let (_, low_id) = router.add_subscription(pattern("sensors/+/data"), QoS::AtMostOnce, "h3");

        match router.unsubscribe(&high_id_1).unwrap() {
            UnsubscribeAction::NoBrokerAction { topic } => {
                assert_topic(&topic, "sensors/+/data");
            }
            action => panic!("expected no broker action, got {action:?}"),
        }

        assert!(router.get_topic_by_id(&high_id_2).is_ok());
        assert!(router.get_topic_by_id(&low_id).is_ok());
    }

    #[test]
    fn unsubscribe_unknown_subscription_returns_not_found() {
        let mut router = TopicRouter::new();
        let (_, id) = router.add_subscription(pattern("sensors/+/data"), QoS::AtMostOnce, "h1");

        router.unsubscribe(&id).unwrap();
        assert_eq!(
            router.unsubscribe(&id).unwrap_err(),
            TopicRouterError::subscription_not_found(id)
        );
    }

    #[test]
    fn unsubscribe_empty_pattern_while_other_patterns_exist_unsubscribes_removed_pattern() {
        let mut router = TopicRouter::new();
        let (_, removed_id) =
            router.add_subscription(pattern("sensors/+/data"), QoS::AtMostOnce, "h1");
        let (_, other_id) = router.add_subscription(pattern("alerts/#"), QoS::AtLeastOnce, "h2");

        match router.unsubscribe(&removed_id).unwrap() {
            UnsubscribeAction::Unsubscribe { topic } => {
                assert_topic(&topic, "sensors/+/data");
            }
            action => panic!("expected broker unsubscribe, got {action:?}"),
        }

        assert!(router.get_topic_by_id(&other_id).is_ok());
    }
}
