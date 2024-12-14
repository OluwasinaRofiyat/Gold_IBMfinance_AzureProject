CREATE OR ALTER PROCEDURE PRD.PopulateAnalysisTables
AS
BEGIN
    DECLARE @execution_time DATETIME = GETDATE();
    DECLARE @status NVARCHAR(50);
    DECLARE @error_message NVARCHAR(MAX);

    BEGIN TRY
        -- Start a transaction
        BEGIN TRANSACTION;

        -- Step 1: Append New Data to Raw Aggregated Data Table
        INSERT INTO PRD.RawAggregatedData (week_start, avg_open, avg_high, avg_low, avg_close, total_volume)
        SELECT 
            CAST(week_start AS DATE) AS week_start,
            AVG(CAST(avg_open AS FLOAT)) AS avg_open,
            AVG(CAST(avg_high AS FLOAT)) AS avg_high,
            AVG(CAST(avg_low AS FLOAT)) AS avg_low,
            AVG(CAST(avg_close AS FLOAT)) AS avg_close,
            SUM(CAST(total_volume AS BIGINT)) AS total_volume
        FROM EDW.financedata AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM PRD.RawAggregatedData AS dest
            WHERE dest.week_start = CAST(src.week_start AS DATE)
        )
        GROUP BY CAST(week_start AS DATE);

        -- Step 2: Append New Data to Trend Analysis Table
        INSERT INTO PRD.TrendAnalysis (week_start, total_volume, moving_avg_volume, volume_trend)
        SELECT 
            src.week_start,
            src.total_volume,
            AVG(src.total_volume) OVER (ORDER BY src.week_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg_volume,
            CASE 
                WHEN ABS(src.total_volume - AVG(src.total_volume) OVER (ORDER BY src.week_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) > 
                     2 * AVG(src.total_volume) OVER (ORDER BY src.week_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                THEN 'Anomaly'
                ELSE 'Normal'
            END AS volume_trend
        FROM PRD.RawAggregatedData AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM PRD.TrendAnalysis AS dest
            WHERE dest.week_start = src.week_start
        );

        -- Step 3: Append New Data to Volume Analysis Table
        INSERT INTO PRD.VolumeAnalysis (week_start, total_volume, moving_avg_volume, prev_week_volume, weekly_percentage_change)
        SELECT 
            src.week_start,
            src.total_volume,
            AVG(src.total_volume) OVER (ORDER BY src.week_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg_volume,
            LAG(src.total_volume) OVER (ORDER BY src.week_start) AS prev_week_volume,
            ROUND(
                (CAST(src.total_volume AS FLOAT) - CAST(LAG(src.total_volume) OVER (ORDER BY src.week_start) AS FLOAT)) / 
                CAST(LAG(src.total_volume) OVER (ORDER BY src.week_start) AS FLOAT) * 100, 2
            ) AS weekly_percentage_change
        FROM PRD.RawAggregatedData AS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM PRD.VolumeAnalysis AS dest
            WHERE dest.week_start = src.week_start
        );

        -- Step 4: Update Metrics Summary Table
        DELETE FROM PRD.MetricsSummary
        WHERE metric_name IN (
            'Total Volume (Last 5 Years)',
            'Average Weekly Volume',
            'Total Anomalies'
        );

        INSERT INTO PRD.MetricsSummary (metric_name, metric_value)
        VALUES
            ('Total Volume (Last 5 Years)', 
             (SELECT SUM(total_volume) FROM PRD.RawAggregatedData WHERE week_start >= DATEADD(YEAR, -5, GETDATE()))),
            ('Average Weekly Volume', 
             (SELECT AVG(total_volume) FROM PRD.RawAggregatedData WHERE week_start >= DATEADD(YEAR, -5, GETDATE()))),
            ('Total Anomalies', 
             (SELECT COUNT(*) FROM PRD.TrendAnalysis WHERE volume_trend = 'Anomaly'));

        -- Log successful execution
        SET @status = 'Succeeded';
        INSERT INTO PRD.ProcedureExecutionLogs (procedure_name, execution_time, status)
        VALUES ('PRD.PopulateAnalysisTables', @execution_time, @status);

        -- Commit the transaction
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback in case of error
        ROLLBACK TRANSACTION;

        -- Log failure
        SET @status = 'Failed';
        SET @error_message = ERROR_MESSAGE();
        INSERT INTO PRD.ProcedureExecutionLogs (procedure_name, execution_time, status, error_message)
        VALUES ('PRD.PopulateAnalysisTables', @execution_time, @status, @error_message);

        -- Re-throw the error
        THROW;
    END CATCH
END;
