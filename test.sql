-- Create the Interpolate function (unchanged)
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.Interpolate(x FLOAT, x1 FLOAT, x2 FLOAT, y1 FLOAT, y2 FLOAT)
RETURNS FLOAT
AS
$$
    CASE 
        WHEN x1 = x2 THEN NULL
        WHEN x < x1 OR x > x2 THEN NULL
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;

-- Create the Extrapolate function (unchanged)
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.Extrapolate(x FLOAT, x1 FLOAT, x2 FLOAT, y1 FLOAT, y2 FLOAT)
RETURNS FLOAT
AS
$$
    CASE 
        WHEN x1 = x2 THEN NULL
        ELSE y2 + (x - x2) * (y1 - y2) / (x1 - x2)
    END
$$;

-- Create the main InterpolatePVTCompletionTest function as a table function
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.InterpolatePVTCompletionTest(completion VARCHAR(32), pressure FLOAT, vrr_date DATE)
RETURNS TABLE (
    PRESSURE FLOAT,
    OIL_FORMATION_VOLUME_FACTOR FLOAT,
    GAS_FORMATION_VOLUME_FACTOR FLOAT,
    WATER_FORMATION_VOLUME_FACTOR FLOAT,
    SOLUTION_GAS_OIL_RATIO FLOAT,
    VISCOSITY_OIL FLOAT,
    VISCOSITY_WATER FLOAT,
    VISCOSITY_GAS FLOAT,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR FLOAT,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR FLOAT
)
AS
$$
WITH 
-- Step 1: Select base data from COMPLETION_PVT_CHARACTERISTICS with the new schema
BasePVTData AS (
    SELECT 
        ID_COMPLETION,
        TEST_DATE,
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR,
        LEAD(TEST_DATE) OVER (PARTITION BY ID_COMPLETION ORDER BY TEST_DATE) AS NEXT_TEST_DATE
    FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS
    WHERE ID_COMPLETION = completion 
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
),
-- Step 2: Compute END_DATE for each record using NEXT_TEST_DATE
PVTwithEndDate AS (
    SELECT 
        ID_COMPLETION,
        TEST_DATE,
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR,
        COALESCE(NEXT_TEST_DATE, '9999-12-31'::DATE) AS END_DATE
    FROM BasePVTData
    WHERE LAST_DAY(vrr_date, 'MONTH') < COALESCE(NEXT_TEST_DATE, '9999-12-31'::DATE)
),
-- Step 3: Find an exact match (equivalent to @ExactMatch)
ExactMatch AS (
    SELECT 
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE PRESSURE = pressure
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
    ORDER BY TEST_DATE DESC
    LIMIT 1
),
-- Step 4: Find the lower bound (equivalent to @Lowerbound)
Lowerbound AS (
    SELECT 
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE PRESSURE < pressure
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
    ORDER BY TEST_DATE DESC
    LIMIT 1
),
-- Step 5: Find the upper bound (equivalent to @Upperbound)
Upperbound AS (
    SELECT 
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE PRESSURE > pressure
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
    ORDER BY TEST_DATE DESC
    LIMIT 1
),
-- Step 6: Find the second bound for extrapolation (equivalent to @SecondBound)
SecondBound AS (
    SELECT 
        PRESSURE,
        OIL_FORMATION_VOLUME_FACTOR,
        GAS_FORMATION_VOLUME_FACTOR,
        WATER_FORMATION_VOLUME_FACTOR,
        SOLUTION_GAS_OIL_RATIO,
        VISCOSITY_OIL,
        VISCOSITY_WATER,
        VISCOSITY_GAS,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM PVTwithEndDate
    WHERE PRESSURE < (SELECT PRESSURE FROM Upperbound)
      AND ID_COMPLETION = completion
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
    ORDER BY TEST_DATE DESC
    LIMIT 1
),
-- Step 7: Compute the interpolated or extrapolated values
InterpolatedValues AS (
    SELECT 
        CASE 
            WHEN (SELECT COUNT(*) FROM ExactMatch) = 1 THEN
                SELECT 
                    PRESSURE,
                    OIL_FORMATION_VOLUME_FACTOR,
                    GAS_FORMATION_VOLUME_FACTOR,
                    WATER_FORMATION_VOLUME_FACTOR,
                    SOLUTION_GAS_OIL_RATIO,
                    VISCOSITY_OIL,
                    VISCOSITY_WATER,
                    VISCOSITY_GAS,
                    INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    INJECTED_WATER_FORMATION_VOLUME_FACTOR
                FROM ExactMatch
            WHEN (SELECT COUNT(*) FROM Lowerbound) = 1 AND (SELECT COUNT(*) FROM Upperbound) = 1 THEN
                SELECT 
                    pressure AS PRESSURE,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Lowerbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound)
                    ) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_OIL FROM Lowerbound),
                        (SELECT VISCOSITY_OIL FROM Upperbound)
                    ) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_WATER FROM Lowerbound),
                        (SELECT VISCOSITY_WATER FROM Upperbound)
                    ) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_GAS FROM Lowerbound),
                        (SELECT VISCOSITY_GAS FROM Upperbound)
                    ) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            WHEN (SELECT COUNT(*) FROM Lowerbound) = 1 AND (SELECT COUNT(*) FROM SecondBound) = 1 THEN
                SELECT 
                    pressure AS PRESSURE,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ) AS OIL_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ) AS GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ) AS WATER_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM SecondBound)
                    ) AS SOLUTION_GAS_OIL_RATIO,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_OIL FROM Upperbound),
                        (SELECT VISCOSITY_OIL FROM SecondBound)
                    ) AS VISCOSITY_OIL,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_WATER FROM Upperbound),
                        (SELECT VISCOSITY_WATER FROM SecondBound)
                    ) AS VISCOSITY_WATER,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_GAS FROM Upperbound),
                        (SELECT VISCOSITY_GAS FROM SecondBound)
                    ) AS VISCOSITY_GAS,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
            WHEN (SELECT COUNT(*) FROM Upperbound) = 1 THEN
                SELECT 
                    PRESSURE,
                    OIL_FORMATION_VOLUME_FACTOR,
                    GAS_FORMATION_VOLUME_FACTOR,
                    WATER_FORMATION_VOLUME_FACTOR,
                    SOLUTION_GAS_OIL_RATIO,
                    VISCOSITY_OIL,
                    VISCOSITY_WATER,
                    VISCOSITY_GAS,
                    INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    INJECTED_WATER_FORMATION_VOLUME_FACTOR
                FROM Upperbound
            ELSE
                SELECT 
                    pressure AS PRESSURE,
                    NULL AS OIL_FORMATION_VOLUME_FACTOR,
                    NULL AS GAS_FORMATION_VOLUME_FACTOR,
                    NULL AS WATER_FORMATION_VOLUME_FACTOR,
                    NULL AS SOLUTION_GAS_OIL_RATIO,
                    NULL AS VISCOSITY_OIL,
                    NULL AS VISCOSITY_WATER,
                    NULL AS VISCOSITY_GAS,
                    NULL AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    NULL AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
        END
),
-- Step 8: Round the results to 5 decimal places
RoundedValues AS (
    SELECT 
        ROUND(PRESSURE, 5) AS PRESSURE,
        ROUND(OIL_FORMATION_VOLUME_FACTOR, 5) AS OIL_FORMATION_VOLUME_FACTOR,
        ROUND(GAS_FORMATION_VOLUME_FACTOR, 5) AS GAS_FORMATION_VOLUME_FACTOR,
        ROUND(WATER_FORMATION_VOLUME_FACTOR, 5) AS WATER_FORMATION_VOLUME_FACTOR,
        ROUND(SOLUTION_GAS_OIL_RATIO, 5) AS SOLUTION_GAS_OIL_RATIO,
        ROUND(VISCOSITY_OIL, 5) AS VISCOSITY_OIL,
        ROUND(VISCOSITY_WATER, 5) AS VISCOSITY_WATER,
        ROUND(VISCOSITY_GAS, 5) AS VISCOSITY_GAS,
        ROUND(INJECTED_GAS_FORMATION_VOLUME_FACTOR, 5) AS INJECTED_GAS_FORMATION_VOLUME_FACTOR,
        ROUND(INJECTED_WATER_FORMATION_VOLUME_FACTOR, 5) AS INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM InterpolatedValues
)
SELECT 
    PRESSURE,
    OIL_FORMATION_VOLUME_FACTOR,
    GAS_FORMATION_VOLUME_FACTOR,
    WATER_FORMATION_VOLUME_FACTOR,
    SOLUTION_GAS_OIL_RATIO,
    VISCOSITY_OIL,
    VISCOSITY_WATER,
    VISCOSITY_GAS,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR
FROM RoundedValues;
$$;
