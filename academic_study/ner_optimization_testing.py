"""
Analytical Study of NER Optimization and Performance Testing
"""
import argparse
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.nlp import (
    ENTITIES_SCHEMA,
    extract_entities,
)
from semantic_medallion_data_platform.common.pyspark import (
    composite_to_hash,
    create_spark_session,
)

logger = get_logger(__name__)

_composite_to_hash_udf = F.udf(lambda *cols: composite_to_hash(*cols), StringType())


def calculate_metrics(
    actual: Dict[str, int], predicted: Dict[str, int]
) -> Dict[str, Dict[str, float]]:
    """
    Calculate precision, recall, and F1-score for each entity type.

    Args:
        actual: Dictionary with actual counts for each entity type
        predicted: Dictionary with predicted counts for each entity type

    Returns:
        Dictionary with precision, recall, and F1-score for each entity type
    """
    metrics = {}

    for entity_type in ["PERSON", "ORG", "LOC"]:
        # Get actual and predicted counts
        a = actual.get(entity_type.lower(), 0)
        p = predicted.get(entity_type, 0)

        # Calculate precision, recall, and F1-score
        precision = p / max(p, 1) if a > 0 else 0
        recall = min(p, a) / max(a, 1) if a > 0 else 0
        f1 = (
            2 * precision * recall / max(precision + recall, 1)
            if precision + recall > 0
            else 0
        )

        metrics[entity_type] = {"precision": precision, "recall": recall, "f1": f1}

    return metrics


def create_summary_visualization(
    avg_metrics: Dict[str, Dict[str, float]], output_dir: str = "academic_study/plots"
) -> None:
    """
    Create a summary visualization of the model's performance.

    Args:
        avg_metrics: Dictionary with average metrics for each entity type
        output_dir: Directory to save plots
    """
    import os

    os.makedirs(output_dir, exist_ok=True)

    # Create a radar chart for overall performance
    plt.figure(figsize=(10, 8))

    # Set up the radar chart
    entity_types = list(avg_metrics.keys())
    metrics = ["precision", "recall", "f1"]

    # Number of variables
    N = len(metrics)

    # What will be the angle of each axis in the plot
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]  # Close the loop

    # Initialize the plot
    ax = plt.subplot(111, polar=True)

    # Draw one axis per variable and add labels
    plt.xticks(angles[:-1], metrics, size=12)

    # Draw the y-axis labels (0-1)
    ax.set_rlabel_position(0)
    plt.yticks([0.2, 0.4, 0.6, 0.8, 1.0], ["0.2", "0.4", "0.6", "0.8", "1.0"], size=10)
    plt.ylim(0, 1)

    # Plot each entity type
    for i, entity_type in enumerate(entity_types):
        values = [
            avg_metrics[entity_type]["precision"],
            avg_metrics[entity_type]["recall"],
            avg_metrics[entity_type]["f1"],
        ]
        values += values[:1]  # Close the loop

        # Plot values
        ax.plot(angles, values, linewidth=2, linestyle="solid", label=entity_type)
        ax.fill(angles, values, alpha=0.1)

    # Add legend
    plt.legend(loc="upper right", bbox_to_anchor=(0.1, 0.1))

    plt.title("NER Model Performance Summary", size=15)
    plt.tight_layout()

    plt.savefig(f"{output_dir}/performance_summary_radar.png")
    plt.close()

    # Create a bar chart for overall performance
    plt.figure(figsize=(12, 8))

    # Set up the bar chart
    x = np.arange(len(entity_types))
    width = 0.25

    # Plot bars for each metric
    precision_values = [avg_metrics[et]["precision"] for et in entity_types]
    recall_values = [avg_metrics[et]["recall"] for et in entity_types]
    f1_values = [avg_metrics[et]["f1"] for et in entity_types]

    plt.bar(x - width, precision_values, width, label="Precision")
    plt.bar(x, recall_values, width, label="Recall")
    plt.bar(x + width, f1_values, width, label="F1-Score")

    plt.xlabel("Entity Type")
    plt.ylabel("Score")
    plt.title("NER Model Performance Summary")
    plt.xticks(x, entity_types)
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add value labels on top of bars
    for i, v in enumerate(precision_values):
        plt.text(i - width, v + 0.02, f"{v:.2f}", ha="center")
    for i, v in enumerate(recall_values):
        plt.text(i, v + 0.02, f"{v:.2f}", ha="center")
    for i, v in enumerate(f1_values):
        plt.text(i + width, v + 0.02, f"{v:.2f}", ha="center")

    plt.savefig(f"{output_dir}/performance_summary_bar.png")
    plt.close()


def create_comparison_plots(
    ground_truth_df: pd.DataFrame,
    predicted_df: pd.DataFrame,
    metrics_df: pd.DataFrame,
    output_dir: str = "academic_study/plots",
) -> None:
    """
    Create plots comparing ground truth and predicted entity counts.

    Args:
        ground_truth_df: DataFrame with ground truth entity counts
        predicted_df: DataFrame with predicted entity counts
        metrics_df: DataFrame with precision, recall, and F1-score for each entity type
        output_dir: Directory to save plots
    """
    import os

    os.makedirs(output_dir, exist_ok=True)

    # 1. Bar chart comparing average entity counts
    plt.figure(figsize=(12, 6))

    entity_types = ["PERSON", "ORG", "LOC"]
    # Handle column name variations (singular/plural)
    ground_truth_means = []
    for et in entity_types:
        et_lower = et.lower()
        # Check for both singular and plural forms
        if et_lower + "s" in ground_truth_df.columns:
            ground_truth_means.append(ground_truth_df[et_lower + "s"].mean())
        elif et_lower in ground_truth_df.columns:
            ground_truth_means.append(ground_truth_df[et_lower].mean())
        else:
            # Fallback to 0 if column doesn't exist
            ground_truth_means.append(0)
            logger.warning(
                f"Column {et_lower} or {et_lower}s not found in ground truth DataFrame"
            )
    predicted_means = [
        predicted_df[et].mean() if et in predicted_df.columns else 0
        for et in entity_types
    ]

    x = np.arange(len(entity_types))
    width = 0.35

    plt.bar(x - width / 2, ground_truth_means, width, label="Ground Truth")
    plt.bar(x + width / 2, predicted_means, width, label="Predicted")

    plt.xlabel("Entity Type")
    plt.ylabel("Average Count")
    plt.title("Average Entity Counts: Ground Truth vs Predicted")
    plt.xticks(x, entity_types)
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    plt.savefig(f"{output_dir}/entity_count_comparison.png")
    plt.close()

    # 2. Heatmap of performance metrics
    plt.figure(figsize=(10, 8))

    # Create a matrix of metrics
    metrics_matrix = np.zeros((len(entity_types), 3))
    for i, et in enumerate(entity_types):
        # Use proper pandas DataFrame access
        if f"{et}_precision" in metrics_df.columns:
            metrics_matrix[i, 0] = metrics_df[f"{et}_precision"].mean()
        if f"{et}_recall" in metrics_df.columns:
            metrics_matrix[i, 1] = metrics_df[f"{et}_recall"].mean()
        if f"{et}_f1" in metrics_df.columns:
            metrics_matrix[i, 2] = metrics_df[f"{et}_f1"].mean()

    plt.imshow(metrics_matrix, cmap="viridis", aspect="auto")
    plt.colorbar(label="Score")

    plt.xticks(np.arange(3), ["Precision", "Recall", "F1-Score"])
    plt.yticks(np.arange(len(entity_types)), entity_types)

    # Add text annotations
    for i in range(len(entity_types)):
        for j in range(3):
            plt.text(
                j,
                i,
                f"{metrics_matrix[i, j]:.2f}",
                ha="center",
                va="center",
                color="white",
            )

    plt.title("NER Performance Metrics by Entity Type")
    plt.tight_layout()

    plt.savefig(f"{output_dir}/performance_metrics_heatmap.png")
    plt.close()

    # 3. Line chart showing entity recognition accuracy across the dataset
    plt.figure(figsize=(14, 7))

    # Calculate accuracy for each sample
    accuracy_data = []
    for i in range(len(ground_truth_df)):
        sample_accuracy = {}
        for et in entity_types:
            # Check for both singular and plural forms
            et_lower = et.lower()
            if et_lower + "s" in ground_truth_df.columns:
                gt = ground_truth_df.iloc[i].get(et_lower + "s", 0)
            else:
                gt = ground_truth_df.iloc[i].get(et_lower, 0)
            pred = predicted_df.iloc[i].get(et, 0) if et in predicted_df.columns else 0
            # Simple accuracy measure: 1 - abs(gt - pred) / max(gt, 1)
            accuracy = (
                max(0, 1 - abs(gt - pred) / max(gt, 1))
                if gt > 0
                else (1 if pred == 0 else 0)
            )
            sample_accuracy[et] = accuracy
        accuracy_data.append(sample_accuracy)

    accuracy_df = pd.DataFrame(accuracy_data)

    # Plot line chart
    for et in entity_types:
        plt.plot(accuracy_df.index, accuracy_df[et], label=et)

    plt.xlabel("Sample Index")
    plt.ylabel("Recognition Accuracy")
    plt.title("Entity Recognition Accuracy Across Dataset")
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.savefig(f"{output_dir}/entity_recognition_accuracy.png")
    plt.close()

    # 4. Confusion matrix for entity types
    plt.figure(figsize=(10, 8))

    # Calculate overall metrics - use proper pandas column access
    overall_precision = np.mean(
        [
            metrics_df[f"{et}_precision"].mean()
            if f"{et}_precision" in metrics_df.columns
            else 0
            for et in entity_types
        ]
    )
    overall_recall = np.mean(
        [
            metrics_df[f"{et}_recall"].mean()
            if f"{et}_recall" in metrics_df.columns
            else 0
            for et in entity_types
        ]
    )
    overall_f1 = np.mean(
        [
            metrics_df[f"{et}_f1"].mean() if f"{et}_f1" in metrics_df.columns else 0
            for et in entity_types
        ]
    )

    # Create a simple confusion-like matrix
    confusion = np.zeros((3, 3))
    for i, et in enumerate(entity_types):
        if f"{et}_precision" in metrics_df.columns:
            confusion[i, 0] = metrics_df[f"{et}_precision"].mean()
        if f"{et}_recall" in metrics_df.columns:
            confusion[i, 1] = metrics_df[f"{et}_recall"].mean()
        if f"{et}_f1" in metrics_df.columns:
            confusion[i, 2] = metrics_df[f"{et}_f1"].mean()

    plt.imshow(confusion, cmap="Blues")

    # Add text annotations
    for i in range(3):
        for j in range(3):
            plt.text(
                j,
                i,
                f"{confusion[i, j]:.2f}",
                ha="center",
                va="center",
                color="black" if confusion[i, j] < 0.7 else "white",
            )

    plt.xticks(np.arange(3), ["Precision", "Recall", "F1-Score"])
    plt.yticks(np.arange(3), entity_types)

    plt.title(
        f"NER Performance Matrix\nOverall: P={overall_precision:.2f}, R={overall_recall:.2f}, F1={overall_f1:.2f}"
    )
    plt.colorbar(label="Score")

    plt.savefig(f"{output_dir}/performance_matrix.png")
    plt.close()


def main() -> None:
    """Main entry point for the script."""
    try:
        # Extract test data for NER optimization
        logger.info("Reading ner_test_data.csv")
        ner_test_data_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("academic_study/data/ner_test_data.csv")
        )
        # Set uid based on data
        ner_test_data_df = ner_test_data_df.withColumn(
            "uid", _composite_to_hash_udf("data")
        )
        ner_test_data_df.show(10)

        # Process ner_test_data_df.data
        extracted_entities = []
        for col in ner_test_data_df.collect():
            uid = col["uid"]
            data = col["data"]
            logger.info(f"Processing data: {data}")
            extracted_entities.append(
                {"uid": uid, "data": data, "entities": extract_entities(data)}
            )

        logger.info("Defining extracted entities from the data")
        extracted_entities_df = spark.createDataFrame(
            extracted_entities,
            schema=StructType(
                [
                    StructField("uid", StringType(), True),
                    StructField("data", StringType(), True),
                    StructField("entities", ENTITIES_SCHEMA, True),
                ]
            ),
        )

        # Explode the entities column to get individual entities
        extracted_entities_df = extracted_entities_df.withColumn(
            "entity", F.explode(F.col("entities"))
        )

        # Flatten the entities structure
        extracted_entities_df = extracted_entities_df.select(
            "uid",
            "data",
            F.col("entity.text").alias("entity_name"),
            F.col("entity.type").alias("entity_type"),
        )

        # Replace the 'GPE' type with 'LOC'
        extracted_entities_df = extracted_entities_df.withColumn(
            "entity_type",
            F.when(F.col("entity_type") == "GPE", "LOC").otherwise(
                F.col("entity_type")
            ),
        )

        # Pivot the extracted_entities_df table so that for each uri we count the number of entity types
        # resulting in a table with columns: uri, data, person, org, loc
        extracted_entities_df = (
            extracted_entities_df.groupBy("uid", "data")
            .pivot("entity_type")
            .agg(F.count("entity_name").alias("count"))
            .fillna(0)  # Fill missing values with 0
        )

        extracted_entities_df.show(10)

        # Convert Spark DataFrames to pandas for easier manipulation and visualization
        logger.info("Converting Spark DataFrames to pandas DataFrames")
        ground_truth_df = ner_test_data_df.toPandas()
        predicted_df = extracted_entities_df.toPandas()

        # Ensure column names are consistent
        # Rename columns in predicted_df if needed (e.g., PERSON -> person)
        column_mapping = {}
        for col in predicted_df.columns:
            if col in ["PERSON", "ORG", "LOC"]:
                column_mapping[col] = col  # Keep original for metrics calculation

        # Calculate metrics for each row
        logger.info("Calculating performance metrics")
        metrics_data = []

        for i, row in ground_truth_df.iterrows():
            # Get the corresponding predicted row
            uid = row["uid"]
            predicted_row = predicted_df[predicted_df["uid"] == uid]

            if not predicted_row.empty:
                # Get actual and predicted counts
                # Handle potential column name variations
                actual = {
                    "person": row.get("persons", row.get("person", 0)),
                    "org": row.get("org", 0),
                    "loc": row.get("loc", 0),
                }

                predicted = {
                    "PERSON": predicted_row["PERSON"].values[0]
                    if "PERSON" in predicted_row.columns
                    else 0,
                    "ORG": predicted_row["ORG"].values[0]
                    if "ORG" in predicted_row.columns
                    else 0,
                    "LOC": predicted_row["LOC"].values[0]
                    if "LOC" in predicted_row.columns
                    else 0,
                }

                # Calculate metrics
                metrics = calculate_metrics(actual, predicted)

                # Add to metrics data
                metrics_row = {
                    "uid": uid,
                    "data": row["data"],
                    "PERSON_precision": metrics["PERSON"]["precision"],
                    "PERSON_recall": metrics["PERSON"]["recall"],
                    "PERSON_f1": metrics["PERSON"]["f1"],
                    "ORG_precision": metrics["ORG"]["precision"],
                    "ORG_recall": metrics["ORG"]["recall"],
                    "ORG_f1": metrics["ORG"]["f1"],
                    "LOC_precision": metrics["LOC"]["precision"],
                    "LOC_recall": metrics["LOC"]["recall"],
                    "LOC_f1": metrics["LOC"]["f1"],
                }
                metrics_data.append(metrics_row)

        # Create metrics DataFrame
        metrics_df = pd.DataFrame(metrics_data)

        # Calculate average metrics
        avg_metrics = {
            "PERSON": {
                "precision": metrics_df["PERSON_precision"].mean(),
                "recall": metrics_df["PERSON_recall"].mean(),
                "f1": metrics_df["PERSON_f1"].mean(),
            },
            "ORG": {
                "precision": metrics_df["ORG_precision"].mean(),
                "recall": metrics_df["ORG_recall"].mean(),
                "f1": metrics_df["ORG_f1"].mean(),
            },
            "LOC": {
                "precision": metrics_df["LOC_precision"].mean(),
                "recall": metrics_df["LOC_recall"].mean(),
                "f1": metrics_df["LOC_f1"].mean(),
            },
        }

        # Print average metrics
        logger.info("Average Performance Metrics:")
        for entity_type, metrics in avg_metrics.items():
            logger.info(
                f"{entity_type}: Precision={metrics['precision']:.2f}, Recall={metrics['recall']:.2f}, F1={metrics['f1']:.2f}"
            )

        # Create visualizations
        logger.info("Creating performance visualizations")
        create_comparison_plots(ground_truth_df, predicted_df, metrics_df)

        # Create summary visualization
        logger.info("Creating summary visualization")
        create_summary_visualization(avg_metrics)

        logger.info(
            "Performance analysis completed. Visualizations saved to academic_study/plots/"
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e  # Re-raise the exception after logging it
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    spark = create_spark_session("ner_optimization_testing")
    parser = argparse.ArgumentParser(description="ner_optimization_testing")

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
