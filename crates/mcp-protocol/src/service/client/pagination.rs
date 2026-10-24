use std::collections::HashSet;

use crate::service::ServiceError;

/// Bounds for a single list-all operation, independent of transport and cache limits.
///
/// Defaults to 1,024 pages and 100,000 items. These are count limits, not byte or
/// time limits. Each page is received and parsed before its item count is checked.
/// Zero pages rejects before sending; zero items allows empty pages only.
/// Cursor cycles are always rejected, including a returned initial resume cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PaginationLimits {
    /// Maximum number of page lookups, including cached responses and empty pages.
    pub max_pages: usize,
    /// Maximum total number of items returned by a successful collection.
    pub max_items: usize,
}

impl Default for PaginationLimits {
    fn default() -> Self {
        Self {
            max_pages: 1024,
            max_items: 100_000,
        }
    }
}

impl PaginationLimits {
    /// Set the maximum number of pages. Zero prevents the first lookup.
    pub fn with_max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages;
        self
    }
    /// Set the maximum item count. Zero allows only empty pages.
    pub fn with_max_items(mut self, max_items: usize) -> Self {
        self.max_items = max_items;
        self
    }
}

pub(super) async fn collect_pages<T, F, Fut>(
    mut cursor: Option<String>,
    limits: PaginationLimits,
    mut fetch: F,
) -> Result<Vec<T>, ServiceError>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<(Vec<T>, Option<String>), ServiceError>>,
{
    let mut seen = HashSet::new();
    if let Some(cursor) = &cursor {
        seen.insert(cursor.clone());
    }
    let mut items = Vec::new();
    for _ in 0..limits.max_pages {
        let (page, next) = fetch(cursor).await?;
        if page.len() > limits.max_items - items.len() {
            return Err(ServiceError::PaginationItemLimitExceeded {
                limit: limits.max_items,
            });
        }
        items.extend(page);
        let Some(next) = next else { return Ok(items) };
        if !seen.insert(next.clone()) {
            return Err(ServiceError::PaginationCursorCycle);
        }
        cursor = Some(next);
    }
    Err(ServiceError::PaginationPageLimitExceeded {
        limit: limits.max_pages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;

    #[tokio::test]
    async fn exact_item_and_page_boundaries_and_zero_empty_pages() {
        let limits = PaginationLimits::default()
            .with_max_pages(2)
            .with_max_items(2);
        let mut calls = 0;
        let result = collect_pages(None, limits, |cursor| {
            calls += 1;
            assert_eq!(
                cursor.as_deref(),
                if calls == 1 { None } else { Some("next") }
            );
            ready(Ok((
                vec![calls],
                if calls == 1 {
                    Some("next".into())
                } else {
                    None
                },
            )))
        })
        .await
        .unwrap();
        assert_eq!(result, [1, 2]);
        let empty = collect_pages::<(), _, _>(None, limits.with_max_items(0), |_| {
            ready(Ok((vec![], None)))
        })
        .await
        .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn unique_empty_pages_stop_before_an_extra_lookup_and_zero_sends_nothing() {
        let mut calls = 0;
        let limits = PaginationLimits::default().with_max_pages(3);
        let error = collect_pages::<(), _, _>(None, limits, |_| {
            calls += 1;
            ready(Ok((vec![], Some(calls.to_string()))))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationPageLimitExceeded { limit: 3 }
        ));
        assert_eq!(calls, 3);
        let error = collect_pages::<(), _, _>(None, limits.with_max_pages(0), |_| {
            panic!("zero must not fetch");
            #[allow(unreachable_code)]
            ready(Ok((vec![], None)))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationPageLimitExceeded { limit: 0 }
        ));
    }

    #[tokio::test]
    async fn detects_self_cycles_multi_cursor_cycles_and_initial_resume_cursor() {
        for (initial, nexts) in [
            (None, vec!["a", "a"]),
            (None, vec!["a", "b", "a"]),
            (Some("resume".to_owned()), vec!["resume"]),
        ] {
            let expected = nexts.len();
            let mut nexts = nexts.into_iter();
            let mut calls = 0;
            let error = collect_pages::<(), _, _>(initial, PaginationLimits::default(), |_| {
                calls += 1;
                ready(Ok((vec![], Some(nexts.next().unwrap().to_owned()))))
            })
            .await
            .unwrap_err();
            assert!(matches!(error, ServiceError::PaginationCursorCycle));
            assert_eq!(calls, expected);
        }
    }

    #[tokio::test]
    async fn excess_items_and_fetch_errors_return_no_partial_collection() {
        let mut calls = 0;
        let error = collect_pages(None, PaginationLimits::default().with_max_items(1), |_| {
            calls += 1;
            ready(Ok((vec![0], Some(calls.to_string()))))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationItemLimitExceeded { limit: 1 }
        ));
        assert_eq!(calls, 2);
        let error = collect_pages::<(), _, _>(None, PaginationLimits::default(), |_| {
            ready(Err(ServiceError::TransportClosed))
        })
        .await
        .unwrap_err();
        assert!(matches!(error, ServiceError::TransportClosed));
        let error = collect_pages(None, PaginationLimits::default().with_max_items(0), |_| {
            ready(Ok((vec![0], None)))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::PaginationItemLimitExceeded { limit: 0 }
        ));
    }
}
