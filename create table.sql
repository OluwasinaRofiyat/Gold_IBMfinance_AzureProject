CREATE TABLE PRD.TrendAnalysis (
    week_start DATE NOT NULL PRIMARY KEY,
    total_volume BIGINT NOT NULL,
    moving_avg_volume FLOAT NULL,
    volume_trend NVARCHAR(50) NULL
);



CREATE TABLE PRD.VolumeAnalysis (
    week_start DATE NOT NULL PRIMARY KEY,
    total_volume BIGINT NOT NULL,
    moving_avg_volume FLOAT NULL,
    prev_week_volume BIGINT NULL,
    weekly_percentage_change FLOAT NULL
);



CREATE TABLE PRD.MetricsSummary (
    metric_name NVARCHAR(50) NOT NULL PRIMARY KEY,
    metric_value FLOAT NULL
);




CREATE TABLE PRD.RawAggregatedData (
    week_start DATE NOT NULL PRIMARY KEY,
    avg_open FLOAT NULL,
    avg_high FLOAT NULL,
    avg_low FLOAT NULL,
    avg_close FLOAT NULL,
    total_volume BIGINT NOT NULL
);


CREATE TABLE PRD.ProcedureExecutionLogs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    procedure_name NVARCHAR(255),
    execution_time DATETIME,
    status NVARCHAR(50),
    error_message NVARCHAR(MAX) NULL
);
