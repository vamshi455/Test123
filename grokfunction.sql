-- Create or replace the InterpolatePVTCompletionTest function
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.InterpolatePVTCompletionTest(
    pressure FLOAT,
    completion VARCHAR(32),
    vrr_date DATE
)
RETURNS TABLE (
    PRESSURE FLOAT,
    OIL_FORMATION_VOLUME_FACTOR FLOAT,
    GAS_FORMATION_VOLUME_FACTOR FLOAT,
    WATER_FORMATION_VOLUME_FACTOR FLOAT,
    SOLUTION_GAS_OIL_RATIO FLOAT,
    VOLATIZED_OIL_GAS_RATIO FLOAT,
    VISCOSITY_OIL FLOAT,
    VISCOSITY_WATER FLOAT,
    VISCOSITY_GAS FLOAT,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR FLOAT,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR FLOAT
)
AS
$$
WITH PVTwithEndDate AS (
    SELECT
        ID_COMPLETION,
        TEST_DATE,
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR,
        COALESCE(
            (SELECT MIN(TEST_DATE)
             FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS cpvt
             WHERE cpvt.TEST_DATE > cpc.TEST_DATE
               AND cpvt.ID_COMPLETION = cpc.ID_COMPLETION),
            '9999-12-31'::DATE
        ) AS END_DATE
    FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS cpc
    WHERE ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date)
),
ExactMatch AS (
    SELECT
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE TEST_DATE = (
        SELECT MAX(TEST_DATE)
        FROM PVTwithEndDate p
        WHERE p.PRESSURE = pressure
          AND p.ID_COMPLETION = completion
          AND p.TEST_DATE <= LAST_DAY(vrr_date)
    )
      AND PRESSURE = pressure
    LIMIT 1
),
LowerBound AS (
    SELECT
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUMEstadardize
    FROM PVTwithEndDate
    WHERE PRESSURE < pressure
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date)
    ORDER BY PRESSURE DESC, TEST_DATE DESC
    LIMIT 1
),
UpperBound AS (
    SELECT
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE PRESSURE > pressure
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date)
    ORDER BY PRESSURE ASC, TEST_DATE DESC
    LIMIT 1
),
SecondBound AS (
    SELECT
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate pvt
    WHERE pvt.PRESSURE = (
        SELECT MIN(pressure)
        FROM PVTwithEndDate p
        WHERE p.PRESSURE > (SELECT PRESSURE FROM UpperBound)
          AND p.ID_COMPLETION = completion
          AND p.TEST_DATE <= LAST_DAY(vrr_date)
    )
      AND pvt.ID_COMPLETION = completion
      AND pvt.TEST_DATE <= LAST_DAY(vrr_date)
    ORDER BY TEST_DATE DESC
    LIMIT 1
),
InterpolatedValues AS (
    -- Exact match
    SELECT
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VOLATIZED_OIL_GAS_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM ExactMatch
    WHERE EXISTS (SELECT 1 FROM ExactMatch)

    UNION ALL

    -- Interpolation (lower and upper bounds exist)
    SELECT
        pressure AS PRESSURE,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM LowerBound),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM UpperBound),
            pressure
        ) AS OIL_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM LowerBound),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM UpperBound),
            pressure
        ) AS GAS_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM LowerBound),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM UpperBound),
            pressure
        ) AS WATER_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM LowerBound),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM UpperBound),
            pressure
        ) AS SOLUTION_GAS_OIL_RATIO,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM LowerBound),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM UpperBound),
            pressure
        ) AS VOLATIZED_OIL_GAS_RATIO,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT VISCOSITY_OIL FROM LowerBound),
            (SELECT VISCOSITY_OIL FROM UpperBound),
            pressure
        ) AS VISCOSITY_OIL,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT VISCOSITY_WATER FROM LowerBound),
            (SELECT VISCOSITY_WATER FROM UpperBound),
            pressure
        ) AS VISCOSITY_WATER,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT VISCOSITY_GAS FROM LowerBound),
            (SELECT VISCOSITY_GAS FROM UpperBound),
            pressure
        ) AS VISCOSITY_GAS,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM LowerBound),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM UpperBound),
            pressure
        ) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Interpolate(
            (SELECT PRESSURE FROM LowerBound),
            (SELECT PRESSURE FROM UpperBound),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM LowerBound),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM UpperBound),
            pressure
        ) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
    WHERE EXISTS (SELECT 1 FROM LowerBound)
      AND EXISTS (SELECT 1 FROM UpperBound)
      AND NOT EXISTS (SELECT 1 FROM ExactMatch)

    UNION ALL

    -- Extrapolation (upper bound and second bound exist, no lower bound or exact match)
    SELECT
        pressure AS PRESSURE,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM UpperBound),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM SecondBound),
            pressure
        ) AS OIL_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM UpperBound),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM SecondBound),
            pressure
        ) AS GAS_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM UpperBound),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM SecondBound),
            pressure
        ) AS WATER_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM UpperBound),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM SecondBound),
            pressure
        ) AS SOLUTION_GAS_OIL_RATIO,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM UpperBound),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM SecondBound),
            pressure
        ) AS VOLATIZED_OIL_GAS_RATIO,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT VISCOSITY_OIL FROM UpperBound),
            (SELECT VISCOSITY_OIL FROM SecondBound),
            pressure
        ) AS VISCOSITY_OIL,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT VISCOSITY_WATER FROM UpperBound),
            (SELECT VISCOSITY_WATER FROM SecondBound),
            pressure
        ) AS VISCOSITY_WATER,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT VISCOSITY_GAS FROM UpperBound),
            (SELECT VISCOSITY_GAS FROM SecondBound),
            pressure
        ) AS VISCOSITY_GAS,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM UpperBound),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM SecondBound),
            pressure
        ) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        RMDE_SAM_ACC.Extrapolate(
            (SELECT PRESSURE FROM UpperBound),
            (SELECT PRESSURE FROM SecondBound),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM UpperBound),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM SecondBound),
            pressure
        ) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
    WHERE EXISTS (SELECT 1 FROM UpperBound)
      AND EXISTS (SELECT 1 FROM SecondBound)
      AND NOT EXISTS (SELECT 1 FROM LowerBound)
      AND NOT EXISTS (SELECT 1 FROM ExactMatch)
      AND (SELECT COUNT(*) FROM PVTwithEndDate WHERE ID_COMPLETION = completion AND PRESSURE > pressure AND TEST_DATE <= LAST_DAY(vrr_date)) > 1

    UNION ALL

    -- No valid data
    SELECT
        pressure AS PRESSURE,
        NULL AS OIL_FORMATION_VOLUME_FACTOR,
        NULL AS GAS_FORMATION_VOLUME_FACTOR,
        NULL AS WATER_FORMATION_VOLUME_FACTOR,
        NULL AS SOLUTION_GAS_OIL_RATIO,
        NULL AS VOLATIZED_OIL_GAS_RATIO,
        NULL AS VISCOSITY_OIL,
        NULL AS VISCOSITY_WATER,
        NULL AS VISCOSITY_GAS,
        NULL AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        NULL AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
    WHERE NOT EXISTS (SELECT 1 FROM ExactMatch)
      AND NOT EXISTS (SELECT 1 FROM LowerBound)
      AND NOT EXISTS (SELECT 1 FROM UpperBound)
)
SELECT
    ROUND(PRESSURE, 5) AS PRESSURE,
    ROUND(OIL_FORMATION_VOLUME_FACTOR, 5) AS OIL_FORMATION_VOLUME_FACTOR,
    ROUND(GAS_FORMATION_VOLUME_FACTOR, 5) AS GAS_FORMATION_VOLUME_FACTOR,
    ROUND(WATER_FORMATION_VOLUME_FACTOR, 5) AS WATER_FORMATION_VOLUME_FACTOR,
    ROUND(SOLUTION_GAS_OIL_RATIO, 5) AS SOLUTION_GAS_OIL_RATIO,
    ROUND(VOLATIZED_OIL_GAS_RATIO, 5) AS VOLATIZED_OIL_GAS_RATIO,
    ROUND(VISCOSITY_OIL, 5) AS VISCOSITY_OIL,
    ROUND(VISCOSITY_WATER, 5) AS VISCOSITY_WATER,
    ROUND(VISCOSITY_GAS, 5) AS VISCOSITY_GAS,
    ROUND(INJECTED_GAS_FORMATION_VOLUME_FACTOR, 5) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
    ROUND(INJECTED_WATER_FORMATION_VOLUME_FACTOR, 5) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
FROM InterpolatedValues
$$;

-- Create or replace the Extrapolate function
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.Extrapolate(
    x1 FLOAT, 
    x2 FLOAT, 
    y1 FLOAT, 
    y2 FLOAT, 
    x FLOAT
)
RETURNS FLOAT
AS
$$
    CASE 
        WHEN x1 = x2 THEN y1
        WHEN x1 IS NULL OR x2 IS NULL OR y1 IS NULL OR y2 IS NULL THEN NULL
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;

-- Create or replace the Interpolate function
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.Interpolate(
    x1 FLOAT, 
    x2 FLOAT, 
    y1 FLOAT, 
    y2 FLOAT, 
    x FLOAT
)
RETURNS FLOAT
AS
$$
    CASE 
        WHEN x1 = x2 THEN y1
        WHEN x1 IS NULL OR x2 IS NULL OR y1 IS NULL OR y2 IS NULL THEN NULL
        WHEN x < LEAST(x1, x2) OR x > GREATEST(x1, x2) THEN NULL
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;
