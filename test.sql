-- Create the Interpolate function with the new schema
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

-- Create the Extrapolate function with the new schema
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.Extrapolate(x FLOAT, x1 FLOAT, x2 FLOAT, y1 FLOAT, y2 FLOAT)
RETURNS FLOAT
AS
$$
    CASE 
        WHEN x1 = x2 THEN NULL
        ELSE y2 + (x - x2) * (y1 - y2) / (x1 - x2)
    END
$$;

-- Create the main InterpolatePVTCompletionTest function with the new schema
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.InterpolatePVTCompletionTest(completion VARCHAR(32), pressure FLOAT, vrr_date DATE)
RETURNS VARIANT
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
        INJECTED_WATER_FORMATION_VOLUME_FACTOR
    FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS
    WHERE ID_COMPLETION = completion 
      AND TEST_DATE <= LAST_DAY(vrr_date, 'MONTH')
),
-- Step 2: Compute END_DATE for each record
PVTwithEndDate AS (
    SELECT 
        b.*,
        COALESCE(
            (SELECT MIN(test_date)
             FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS cpt
             WHERE cpt.TEST_DATE > b.TEST_DATE 
               AND cpt.ID_COMPLETION = b.ID_COMPLETION),
            '9999-12-31'::DATE
        ) AS END_DATE
    FROM BasePVTData b
    WHERE LAST_DAY(vrr_date, 'MONTH') < COALESCE(
        (SELECT MIN(test_date)
         FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS cpt
         WHERE cpt.TEST_DATE > b.TEST_DATE 
           AND cpt.ID_COMPLETION = b.ID_COMPLETION),
        '9999-12-31'::DATE
    )
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
                OBJECT_CONSTRUCT(
                    'PRESSURE', (SELECT PRESSURE FROM ExactMatch),
                    'OIL_FORMATION_VOLUME_FACTOR', (SELECT OIL_FORMATION_VOLUME_FACTOR FROM ExactMatch),
                    'GAS_FORMATION_VOLUME_FACTOR', (SELECT GAS_FORMATION_VOLUME_FACTOR FROM ExactMatch),
                    'WATER_FORMATION_VOLUME_FACTOR', (SELECT WATER_FORMATION_VOLUME_FACTOR FROM ExactMatch),
                    'SOLUTION_GAS_OIL_RATIO', (SELECT SOLUTION_GAS_OIL_RATIO FROM ExactMatch),
                    'VISCOSITY_OIL', (SELECT VISCOSITY_OIL FROM ExactMatch),
                    'VISCOSITY_WATER', (SELECT VISCOSITY_WATER FROM ExactMatch),
                    'VISCOSITY_GAS', (SELECT VISCOSITY_GAS FROM ExactMatch),
                    'INJECTED_GAS_FORMATION_VOLUME_FACTOR', (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM ExactMatch),
                    'INJECTED_WATER_FORMATION_VOLUME_FACTOR', (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM ExactMatch)
                )
            WHEN (SELECT COUNT(*) FROM Lowerbound) = 1 AND (SELECT COUNT(*) FROM Upperbound) = 1 THEN
                OBJECT_CONSTRUCT(
                    'PRESSURE', pressure,
                    'OIL_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ),
                    'GAS_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ),
                    'WATER_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ),
                    'SOLUTION_GAS_OIL_RATIO', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Lowerbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound)
                    ),
                    'VISCOSITY_OIL', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_OIL FROM Lowerbound),
                        (SELECT VISCOSITY_OIL FROM Upperbound)
                    ),
                    'VISCOSITY_WATER', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_WATER FROM Lowerbound),
                        (SELECT VISCOSITY_WATER FROM Upperbound)
                    ),
                    'VISCOSITY_GAS', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT VISCOSITY_GAS FROM Lowerbound),
                        (SELECT VISCOSITY_GAS FROM Upperbound)
                    ),
                    'INJECTED_GAS_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    ),
                    'INJECTED_WATER_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Interpolate(
                        pressure,
                        (SELECT PRESSURE FROM Lowerbound),
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Lowerbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound)
                    )
                )
            WHEN (SELECT COUNT(*) FROM Lowerbound) = 1 AND (SELECT COUNT(*) FROM SecondBound) = 1 THEN
                OBJECT_CONSTRUCT(
                    'PRESSURE', pressure,
                    'OIL_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ),
                    'GAS_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ),
                    'WATER_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ),
                    'SOLUTION_GAS_OIL_RATIO', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM SecondBound)
                    ),
                    'VISCOSITY_OIL', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_OIL FROM Upperbound),
                        (SELECT VISCOSITY_OIL FROM SecondBound)
                    ),
                    'VISCOSITY_WATER', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_WATER FROM Upperbound),
                        (SELECT VISCOSITY_WATER FROM SecondBound)
                    ),
                    'VISCOSITY_GAS', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT VISCOSITY_GAS FROM Upperbound),
                        (SELECT VISCOSITY_GAS FROM SecondBound)
                    ),
                    'INJECTED_GAS_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    ),
                    'INJECTED_WATER_FORMATION_VOLUME_FACTOR', RMDE_SAM_ACC.Extrapolate(
                        pressure,
                        (SELECT PRESSURE FROM Upperbound),
                        (SELECT PRESSURE FROM SecondBound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM SecondBound)
                    )
                )
            WHEN (SELECT COUNT(*) FROM Upperbound) = 1 THEN
                OBJECT_CONSTRUCT(
                    'PRESSURE', (SELECT PRESSURE FROM Upperbound),
                    'OIL_FORMATION_VOLUME_FACTOR', (SELECT OIL_FORMATION_VOLUME_FACTOR FROM Upperbound),
                    'GAS_FORMATION_VOLUME_FACTOR', (SELECT GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                    'WATER_FORMATION_VOLUME_FACTOR', (SELECT WATER_FORMATION_VOLUME_FACTOR FROM Upperbound),
                    'SOLUTION_GAS_OIL_RATIO', (SELECT SOLUTION_GAS_OIL_RATIO FROM Upperbound),
                    'VISCOSITY_OIL', (SELECT VISCOSITY_OIL FROM Upperbound),
                    'VISCOSITY_WATER', (SELECT VISCOSITY_WATER FROM Upperbound),
                    'VISCOSITY_GAS', (SELECT VISCOSITY_GAS FROM Upperbound),
                    'INJECTED_GAS_FORMATION_VOLUME_FACTOR', (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM Upperbound),
                    'INJECTED_WATER_FORMATION_VOLUME_FACTOR', (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM Upperbound)
                )
            ELSE
                OBJECT_CONSTRUCT(
                    'PRESSURE', pressure,
                    'OIL_FORMATION_VOLUME_FACTOR', NULL,
                    'GAS_FORMATION_VOLUME_FACTOR', NULL,
                    'WATER_FORMATION_VOLUME_FACTOR', NULL,
                    'SOLUTION_GAS_OIL_RATIO', NULL,
                    'VISCOSITY_OIL', NULL,
                    'VISCOSITY_WATER', NULL,
                    'VISCOSITY_GAS', NULL,
                    'INJECTED_GAS_FORMATION_VOLUME_FACTOR', NULL,
                    'INJECTED_WATER_FORMATION_VOLUME_FACTOR', NULL
                )
        END AS result
)
SELECT 
    OBJECT_CONSTRUCT(
        'PRESSURE', ROUND(result:PRESSURE::FLOAT, 5),
        'OIL_FORMATION_VOLUME_FACTOR', ROUND(result:OIL_FORMATION_VOLUME_FACTOR::FLOAT, 5),
        'GAS_FORMATION_VOLUME_FACTOR', ROUND(result:GAS_FORMATION_VOLUME_FACTOR::FLOAT, 5),
        'WATER_FORMATION_VOLUME_FACTOR', ROUND(result:WATER_FORMATION_VOLUME_FACTOR::FLOAT, 5),
        'SOLUTION_GAS_OIL_RATIO', ROUND(result:SOLUTION_GAS_OIL_RATIO::FLOAT, 5),
        'VISCOSITY_OIL', ROUND(result:VISCOSITY_OIL::FLOAT, 5),
        'VISCOSITY_WATER', ROUND(result:VISCOSITY_WATER::FLOAT, 5),
        'VISCOSITY_GAS', ROUND(result:VISCOSITY_GAS::FLOAT, 5),
        'INJECTED_GAS_FORMATION_VOLUME_FACTOR', ROUND(result:INJECTED_GAS_FORMATION_VOLUME_FACTOR::FLOAT, 5),
        'INJECTED_WATER_FORMATION_VOLUME_FACTOR', ROUND(result:INJECTED_WATER_FORMATION_VOLUME_FACTOR::FLOAT, 5)
    ) AS rounded_result
FROM InterpolatedValues;
$$;
