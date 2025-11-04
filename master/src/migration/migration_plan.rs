use crate::migration::dto::MigrationPlan;
use std::fmt;

/// Represents the execution status of a migration plan
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationStatus {
    /// Migration plan has been created but not executed
    Pending,
    /// Migration is currently in progress
    InProgress,
    /// Migration has been completed successfully
    Completed,
    /// Migration failed with errors
    Failed(String),
    /// Migration was cancelled
    Cancelled,
}

/// Extended migration plan with execution status and metadata
#[derive(Debug, Clone)]
pub struct ExtendedMigrationPlan {
    /// The base migration plan
    pub plan: MigrationPlan,
    /// Current status of the migration
    pub status: MigrationStatus,
    /// Timestamp when the plan was created
    pub created_at: i64,
    /// Timestamp when the migration started (if applicable)
    pub started_at: Option<i64>,
    /// Timestamp when the migration completed (if applicable)
    pub completed_at: Option<i64>,
    /// Progress information (completed operations / total operations)
    pub progress: (usize, usize),
}

impl ExtendedMigrationPlan {
    /// Creates a new extended migration plan with Pending status
    pub fn new(plan: MigrationPlan) -> Self {
        Self {
            plan,
            status: MigrationStatus::Pending,
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            progress: (0, 0),
        }
    }

    /// Starts the migration
    pub fn start(&mut self) {
        self.status = MigrationStatus::InProgress;
        self.started_at = Some(chrono::Utc::now().timestamp());
        self.progress.1 = self.plan.operation_count();
    }

    /// Marks the migration as completed
    pub fn complete(&mut self) {
        self.status = MigrationStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self.progress.0 = self.plan.operation_count();
    }

    /// Marks the migration as failed with an error message
    pub fn fail(&mut self, error: String) {
        self.status = MigrationStatus::Failed(error);
        self.completed_at = Some(chrono::Utc::now().timestamp());
    }

    /// Updates the progress of the migration
    pub fn update_progress(&mut self, completed: usize) {
        self.progress.0 = completed;
    }

    /// Returns the progress percentage (0-100)
    pub fn progress_percentage(&self) -> f64 {
        if self.progress.1 == 0 {
            0.0
        } else {
            (self.progress.0 as f64 / self.progress.1 as f64) * 100.0
        }
    }

    /// Returns true if the migration is completed or failed
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            MigrationStatus::Completed | MigrationStatus::Failed(_) | MigrationStatus::Cancelled
        )
    }
}

impl fmt::Display for ExtendedMigrationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Migration Plan: {} operations, {} unchanged, {} total ({}%)",
            self.plan.operation_count(),
            self.plan.unchanged_objects,
            self.plan.total_objects,
            self.progress_percentage()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::dto::{MigrationOperation, MigrationPlan};

    fn create_test_plan() -> MigrationPlan {
        let mut plan = MigrationPlan::new();
        plan.add_operation(MigrationOperation {
            prev_node: "node1".to_string(),
            new_node: "node2".to_string(),
            object_key: "key1".to_string(),
            bucket_name: "bucket1".to_string(),
        });
        plan.increment_unchanged();
        plan
    }

    #[test]
    fn test_extended_plan_creation() {
        let base_plan = create_test_plan();
        let extended_plan = ExtendedMigrationPlan::new(base_plan);

        assert!(matches!(extended_plan.status, MigrationStatus::Pending));
        assert!(extended_plan.created_at > 0);
        assert!(extended_plan.started_at.is_none());
        assert!(extended_plan.completed_at.is_none());
        assert_eq!(extended_plan.progress, (0, 0));
    }

    #[test]
    fn test_plan_start() {
        let base_plan = create_test_plan();
        let mut extended_plan = ExtendedMigrationPlan::new(base_plan);
        extended_plan.start();

        assert!(matches!(extended_plan.status, MigrationStatus::InProgress));
        assert!(extended_plan.started_at.is_some());
        assert_eq!(extended_plan.progress.1, 1);
    }

    #[test]
    fn test_plan_completion() {
        let base_plan = create_test_plan();
        let mut extended_plan = ExtendedMigrationPlan::new(base_plan);
        extended_plan.start();
        extended_plan.complete();

        assert!(matches!(extended_plan.status, MigrationStatus::Completed));
        assert!(extended_plan.completed_at.is_some());
        assert_eq!(extended_plan.progress.0, 1);
    }

    #[test]
    fn test_progress_percentage() {
        let base_plan = create_test_plan();
        let mut extended_plan = ExtendedMigrationPlan::new(base_plan);
        extended_plan.start();

        assert_eq!(extended_plan.progress_percentage(), 0.0);

        extended_plan.update_progress(1);
        assert_eq!(extended_plan.progress_percentage(), 100.0);
    }

    #[test]
    fn test_is_finished() {
        let base_plan = create_test_plan();
        let mut extended_plan = ExtendedMigrationPlan::new(base_plan);

        assert!(!extended_plan.is_finished());

        extended_plan.start();
        assert!(!extended_plan.is_finished());

        extended_plan.complete();
        assert!(extended_plan.is_finished());

        let mut failed_plan = ExtendedMigrationPlan::new(create_test_plan());
        failed_plan.fail("Test error".to_string());
        assert!(failed_plan.is_finished());
    }
}
