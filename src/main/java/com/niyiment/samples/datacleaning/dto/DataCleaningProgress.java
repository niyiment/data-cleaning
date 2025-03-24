
package com.niyiment.samples.datacleaning.dto;

import java.util.List;


public record DataCleaningProgress(
    double progressPercentage,
    String currentTask,
    List<String> completedTasks
) {
}
