#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn can_transition_to(self, next: TaskStatus) -> bool {
        matches!(
            (self, next),
            (TaskStatus::Queued, TaskStatus::Running)
                | (TaskStatus::Queued, TaskStatus::Cancelled)
                | (TaskStatus::Running, TaskStatus::Completed)
                | (TaskStatus::Running, TaskStatus::Failed)
                | (TaskStatus::Running, TaskStatus::Cancelled)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_machine_has_no_terminal_transitions() {
        assert!(TaskStatus::Queued.can_transition_to(TaskStatus::Running));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Completed));
        assert!(!TaskStatus::Completed.can_transition_to(TaskStatus::Running));
        assert!(!TaskStatus::Failed.can_transition_to(TaskStatus::Completed));
        assert!(!TaskStatus::Cancelled.can_transition_to(TaskStatus::Running));
    }
}
