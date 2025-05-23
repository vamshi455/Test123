CREATE OR REPLACE FUNCTION vrr.InterpolatePVTCompletionTest(
    "pressure" FLOAT,
    "completion" VARCHAR(32),
    "vrr_date" DATE
)
RETURNS TABLE (
    "pressure" FLOAT,
    "OIL_FORMATION_VOLUME_FACTOR" FLOAT,
    "GAS_FORMATION_VOLUME_FACTOR" FLOAT,
    "WATER_FORMATION_VOLUME_FACTOR" FLOAT,
    "SOLUTION_GAS_OIL_RATIO" FLOAT,
    "VOLATIZED_OIL_GAS_RATIO" FLOAT,
    "VISCOSITY_OIL" FLOAT,
    "VISCOSITY_WATER" FLOAT,
    "VISCOSITY_GAS" FLOAT,
    "INJECTED_GAS_FORMATION_VOLUME_FACTOR" FLOAT,
    "INJECTED_WATER_FORMATION_VOLUME_FACTOR" FLOAT
)
LANGUAGE SQL
AS
$$
-- Note: This is the main function body
WITH 
-- Create temporary tables as CTEs (Common Table Expressions) instead of table variables
PVTWithEndDate AS (
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
        COALESCE(LEAD(TEST_DATE) OVER (PARTITION BY ID_COMPLETION ORDER BY TEST_DATE), 
                '9999-12-31'::DATE) AS END_DATE
    FROM vrr.COMPLETION_PVT_CHARACTERISTICS
),

-- Exact pressure match data
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
    FROM PVTWithEndDate
    WHERE PRESSURE = "pressure"
    AND ID_COMPLETION = "completion"
    AND TEST_DATE <= LAST_DAY("vrr_date") -- Snowflake equivalent to EOMONTH
    ORDER BY TEST_DATE DESC
    LIMIT 1
),

-- Lower bound pressure data
Lowerbound AS (
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
    FROM PVTWithEndDate
    WHERE PRESSURE = (
        SELECT MAX(PRESSURE)
        FROM PVTWithEndDate
        WHERE PRESSURE < "pressure"
        AND ID_COMPLETION = "completion"
        AND TEST_DATE <= LAST_DAY("vrr_date")
    )
    AND ID_COMPLETION = "completion"
    AND TEST_DATE <= LAST_DAY("vrr_date")
    ORDER BY TEST_DATE DESC
    LIMIT 1
),

-- Upper bound pressure data
Upperbound AS (
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
    FROM PVTWithEndDate
    WHERE PRESSURE = (
        SELECT MIN(PRESSURE)
        FROM PVTWithEndDate
        WHERE PRESSURE > "pressure"
        AND ID_COMPLETION = "completion"
        AND TEST_DATE <= LAST_DAY("vrr_date")
    )
    AND ID_COMPLETION = "completion"
    AND TEST_DATE <= LAST_DAY("vrr_date")
    ORDER BY TEST_DATE DESC
    LIMIT 1
),

-- Second lower bound for extrapolation
SecondLowerBound AS (
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
    FROM PVTWithEndDate
    WHERE PRESSURE = (
        SELECT MAX(PRESSURE)
        FROM PVTWithEndDate p
        WHERE p.PRESSURE < (SELECT l.PRESSURE FROM Lowerbound l LIMIT 1)
        AND p.ID_COMPLETION = "completion"
        AND p.TEST_DATE <= LAST_DAY("vrr_date")
    )
    AND ID_COMPLETION = "completion"
    AND TEST_DATE <= LAST_DAY("vrr_date")
    AND EXISTS (SELECT 1 FROM Lowerbound)
    ORDER BY TEST_DATE DESC
    LIMIT 1
),

-- Second upper bound for extrapolation
SecondUpperBound AS (
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
    FROM PVTWithEndDate
    WHERE PRESSURE = (
        SELECT MIN(PRESSURE)
        FROM PVTWithEndDate p
        WHERE p.PRESSURE > (SELECT u.PRESSURE FROM Upperbound u LIMIT 1)
        AND p.ID_COMPLETION = "completion"
        AND p.TEST_DATE <= LAST_DAY("vrr_date")
    )
    AND ID_COMPLETION = "completion"
    AND TEST_DATE <= LAST_DAY("vrr_date")
    AND EXISTS (SELECT 1 FROM Upperbound)
    ORDER BY TEST_DATE DESC
    LIMIT 1
),

-- Combine second bounds
SecondBound AS (
    SELECT * FROM SecondLowerBound
    UNION ALL
    SELECT * FROM SecondUpperBound
),

-- Count records for each scenario to determine interpolation/extrapolation approach
Counts AS (
    SELECT
      "pressure" AS target_pressure,
      (SELECT COUNT(*) FROM ExactMatch) AS has_exact_match,
      (SELECT COUNT(*) FROM Lowerbound) AS has_lower_bound,
      (SELECT COUNT(*) FROM Upperbound) AS has_upper_bound,
      (SELECT COUNT(*) FROM SecondBound) AS has_second_bound,
      
      -- Get lower and upper bound values for calculations
      (SELECT PRESSURE FROM Lowerbound LIMIT 1) AS lower_pressure,
      (SELECT PRESSURE FROM Upperbound LIMIT 1) AS upper_pressure,
      (SELECT PRESSURE FROM SecondBound LIMIT 1) AS second_pressure,
      
      -- Interpolation ratio
      CASE 
        WHEN (SELECT COUNT(*) FROM Lowerbound) > 0 AND (SELECT COUNT(*) FROM Upperbound) > 0
        THEN ("pressure" - (SELECT PRESSURE FROM Lowerbound LIMIT 1)) / 
             ((SELECT PRESSURE FROM Upperbound LIMIT 1) - (SELECT PRESSURE FROM Lowerbound LIMIT 1))
        ELSE 0
      END AS interp_ratio
),

-- Calculate interpolated/extrapolated values
InterpolatedValues AS (
    -- Case 1: Exact match exists
    SELECT * FROM ExactMatch WHERE (SELECT has_exact_match FROM Counts) > 0
    
    UNION ALL
    
    -- Case 2: Interpolation between upper and lower bounds
    SELECT
        "pressure",
        -- Oil formation volume factor interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Lowerbound LIMIT 1),
            (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound LIMIT 1)
        ) AS OIL_FORMATION_VOLUME_FACTOR,
        
        -- Gas formation volume factor interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound LIMIT 1),
            (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound LIMIT 1)
        ) AS GAS_FORMATION_VOLUME_FACTOR,
        
        -- Water formation volume factor interpolation 
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound LIMIT 1),
            (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound LIMIT 1)
        ) AS WATER_FORMATION_VOLUME_FACTOR,
        
        -- Solution gas-oil ratio interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM Lowerbound LIMIT 1),
            (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound LIMIT 1)
        ) AS SOLUTION_GAS_OIL_RATIO,
        
        -- Volatized oil-gas ratio interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM Lowerbound LIMIT 1),
            (SELECT VOLATIZED_OIL_GAS_RATIO FROM Upperbound LIMIT 1)
        ) AS VOLATIZED_OIL_GAS_RATIO,
        
        -- Oil viscosity interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT VISCOSITY_OIL FROM Lowerbound LIMIT 1),
            (SELECT VISCOSITY_OIL FROM Upperbound LIMIT 1)
        ) AS VISCOSITY_OIL,
        
        -- Water viscosity interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT VISCOSITY_WATER FROM Lowerbound LIMIT 1),
            (SELECT VISCOSITY_WATER FROM Upperbound LIMIT 1)
        ) AS VISCOSITY_WATER,
        
        -- Gas viscosity interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT VISCOSITY_GAS FROM Lowerbound LIMIT 1),
            (SELECT VISCOSITY_GAS FROM Upperbound LIMIT 1)
        ) AS VISCOSITY_GAS,
        
        -- Injected gas formation volume factor interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound LIMIT 1),
            (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound LIMIT 1)
        ) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        
        -- Injected water formation volume factor interpolation
        vrr.Interpolate(
            "pressure",
            (SELECT lower_pressure FROM Counts),
            (SELECT upper_pressure FROM Counts),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound LIMIT 1),
            (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound LIMIT 1)
        ) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
        
    WHERE (SELECT has_exact_match FROM Counts) = 0
      AND (SELECT has_lower_bound FROM Counts) > 0
      AND (SELECT has_upper_bound FROM Counts) > 0
    
    UNION ALL
    
    -- Case 3: Only lower bound exists (use that)
    SELECT * FROM Lowerbound
    WHERE (SELECT has_exact_match FROM Counts) = 0
      AND (SELECT has_lower_bound FROM Counts) > 0
      AND (SELECT has_upper_bound FROM Counts) = 0
    
    UNION ALL
    
    -- Case 4: Only upper bound exists (use that)
    SELECT * FROM Upperbound
    WHERE (SELECT has_exact_match FROM Counts) = 0
      AND (SELECT has_lower_bound FROM Counts) = 0 
      AND (SELECT has_upper_bound FROM Counts) > 0
    
    UNION ALL
    
    -- Case 5: No matches at all (return nulls)
    SELECT
        "pressure",
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
    WHERE (SELECT has_exact_match FROM Counts) = 0
      AND (SELECT has_lower_bound FROM Counts) = 0
      AND (SELECT has_upper_bound FROM Counts) = 0
      AND NOT EXISTS (SELECT 1 FROM InterpolatedValues)
)

-- Final result with rounded values
SELECT
    ROUND("pressure", 5) AS "pressure",
    ROUND(OIL_FORMATION_VOLUME_FACTOR, 5) AS "OIL_FORMATION_VOLUME_FACTOR",
    ROUND(GAS_FORMATION_VOLUME_FACTOR, 5) AS "GAS_FORMATION_VOLUME_FACTOR", 
    ROUND(WATER_FORMATION_VOLUME_FACTOR, 5) AS "WATER_FORMATION_VOLUME_FACTOR",
    ROUND(SOLUTION_GAS_OIL_RATIO, 5) AS "SOLUTION_GAS_OIL_RATIO",
    ROUND(VOLATIZED_OIL_GAS_RATIO, 5) AS "VOLATIZED_OIL_GAS_RATIO",
    ROUND(VISCOSITY_OIL, 5) AS "VISCOSITY_OIL",
    ROUND(VISCOSITY_WATER, 5) AS "VISCOSITY_WATER",
    ROUND(VISCOSITY_GAS, 5) AS "VISCOSITY_GAS",
    ROUND(INJECTED_GAS_FORMATION_VOLUME_FACTOR, 5) AS "INJECTED_GAS_FORMATION_VOLUME_FACTOR",
    ROUND(INJECTED_WATER_FORMATION_VOLUME_FACTOR, 5) AS "INJECTED_WATER_FORMATION_VOLUME_FACTOR"
FROM InterpolatedValues
WHERE ROWNUM() = 1
$$;

-- Create helper function for interpolation (equivalent to vrr.Interpolate used in original)
CREATE OR REPLACE FUNCTION vrr.Interpolate(
    "target_value" FLOAT,
    "x1" FLOAT,
    "x2" FLOAT,
    "y1" FLOAT,
    "y2" FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT "y1" + ("target_value" - "x1") * ("y2" - "y1") / ("x2" - "x1")
$$;

-- Create helper function for extrapolation (equivalent to vrr.Extrapolate used in original)
CREATE OR REPLACE FUNCTION vrr.Extrapolate(
    "target_value" FLOAT,
    "x1" FLOAT,
    "x2" FLOAT,
    "y1" FLOAT,
    "y2" FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT "y1" + ("target_value" - "x1") * ("y2" - "y1") / ("x2" - "x1")
$$;
