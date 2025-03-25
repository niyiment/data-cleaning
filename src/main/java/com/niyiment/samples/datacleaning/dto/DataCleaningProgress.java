
package com.niyiment.samples.datacleaning.dto;

import java.util.List;


public record DataCleaningProgress(
    Double progressPercentage,
    String currentTask,
    List<String> completedTasks
) {
}
