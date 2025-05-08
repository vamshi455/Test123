-- VRR CALCULATIONS (OILFIELD STANDARD) --
CREATE OR REPLACE VIEW vr.PATTERN_VRR_VIEW
AS
WITH 
-- CTE 1: Calculate the end date (DATUM) for pressure data
Pressure_End_Dates AS (
    SELECT
        ID_PATTERN,
        DATE,
        PRESSURE,
        COALESCE(
            LEAD(DATE, 1) OVER (PARTITION BY ID_PATTERN ORDER BY DATE),
            TO_TIMESTAMP('9999-12-31 00:00:00')
        ) AS DATUM
    FROM RMDE_SAM_ACC.PATTERN_PRESSURE
),

-- CTE 2: Join daily_volume with pattern_contribution_factor to get ID_PATTERN
Daily_Volume_With_Pattern AS (
    SELECT 
        pcf.ID_PATTERN,
        dv.COMPLETION_ID,
        dv.PROD_DATE AS DATE,
        dv.THEOR_OIL_VOL_STB,
        dv.THEOR_WATER_VOL_STB,
        dv.THEOR_GAS_VOL_KSCF,
        dv.CUMULATIVE_INJECTION_VOLUME_res_bbl,
        dv.CUMULATIVE_PRODUCTION_VOLUME_res_bbl
    FROM TRUSTED_DB.PRODUCTION_VOLUME.PRODUCTION_VOLUMES_DAILY_OILFIELD dv
    JOIN RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR pcf
        ON dv.COMPLETION_ID = pcf.ID_COMPLETION
        AND dv.PROD_DATE >= pcf.EFFECT_DATE
        -- Ensure we pick the most recent EFFECT_DATE for the pattern contribution
        AND pcf.EFFECT_DATE = (
            SELECT MAX(EFFECT_DATE)
            FROM RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR pcf2
            WHERE pcf2.ID_COMPLETION = dv.COMPLETION_ID
            AND dv.PROD_DATE >= pcf2.EFFECT_DATE
        )
),

-- CTE 3: Filter daily volume with pressure data and compute interpolated pressure using InterpolatePVTCompletionTest
Filtered_Daily_Volume AS (
    SELECT 
        dvwp.ID_PATTERN,
        dvwp.COMPLETION_ID,
        dvwp.DATE,
        dvwp.THEOR_OIL_VOL_STB,
        dvwp.THEOR_WATER_VOL_STB,
        dvwp.THEOR_GAS_VOL_KSCF,
        dvwp.CUMULATIVE_INJECTION_VOLUME_res_bbl,
        dvwp.CUMULATIVE_PRODUCTION_VOLUME_res_bbl,
        RMDE_SAM_ACC.InterpolatePVTCompletionTest(
            dvwp.COMPLETION_ID, 
            ped.PRESSURE, 
            dvwp.DATE
        ) AS PRESSURE_DATE
    FROM Daily_Volume_With_Pattern dvwp
    JOIN Pressure_End_Dates ped
        ON dvwp.ID_PATTERN = ped.ID_PATTERN
        AND dvwp.DATE >= ped.DATE
        AND dvwp.DATE < ped.DATUM
),

-- CTE 4: Match split factors with the most recent effective date
Split_Factors_Matched AS (
    SELECT 
        sf.*,
        fdv.ID_PATTERN,
        fdv.DATE AS EFFECT_DATE
    FROM RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR sf
    JOIN Filtered_Daily_Volume fdv
        ON sf.ID_PATTERN = fdv.ID_PATTERN
        AND fdv.DATE <= (
            SELECT MAX(EFFECT_DATE)
            FROM RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR pcf
            WHERE pcf.ID_PATTERN = sf.ID_PATTERN
            AND fdv.DATE <= pcf.EFFECT_DATE
        )
),

-- CTE 5: Aggregate the data
Aggregated_Data AS (
    SELECT
        sfm.ID_PATTERN,
        sfm.EFFECT_DATE AS DATE,
        fdv.PRESSURE_DATE AS PRESSURE,
        COALESCE(fdv.THEOR_OIL_VOL_STB, 0) AS THEOR_OIL_VOL_STB,
        COALESCE(fdv.THEOR_WATER_VOL_STB, 0) AS THEOR_WATER_VOL_STB,
        COALESCE(fdv.THEOR_GAS_VOL_KSCF * 1000, 0) AS THEOR_GAS_VOL_KSCF,  -- kscf to scf
        sfm.*,
        fdv.CUMULATIVE_INJECTION_VOLUME_res_bbl,
        fdv.CUMULATIVE_PRODUCTION_VOLUME_res_bbl
    FROM Split_Factors_Matched sfm
    JOIN Filtered_Daily_Volume fdv
        ON sfm.ID_PATTERN = fdv.ID_PATTERN
        AND sfm.EFFECT_DATE = fdv.DATE
)

-- Final SELECT statement
SELECT
    CONCAT(agg.ID_PATTERN, TO_CHAR(agg.DATE, 'DD-MON-YYYY')) AS ID_PATTERN,
    agg.DATE,
    agg.PRESSURE,
    COALESCE(agg.CUMULATIVE_INJECTION_VOLUME_res_bbl / NULLIF(agg.CUMULATIVE_PRODUCTION_VOLUME_res_bbl, 0), 0) AS VRR_Injection_Volume_Production_Volume,
    -- OIL VOLUME (stb)
    agg.THEOR_OIL_VOL_STB,
    -- FREE GAS
    agg.THEOR_WATER_VOL_STB,
    agg.CUMULATIVE_PRODUCTION_VOLUME_res_bbl AS PRODUCTION_VOLUME_res_bbl,
    agg.GAS_FORMATION_VOLUME_FACTOR,
    agg.WATER_FORMATION_VOLUME_FACTOR,
    agg.THEOR_WATER_INJ_VOL_STB AS GAS_INJ_VOLUME_stb,
    agg.THEOR_WATER_INJ_VOL_STB AS WATER_INJ_VOLUME_stb,
    agg.INJECTED_WATER_FORMATION_VOLUME_FACTOR AS WATER_INJ_VOLUME,
    agg.INJECTED_GAS_FORMATION_VOLUME_FACTOR AS GAS_WELL_GAS_VOLUME,
    agg.THEOR_GAS_VOL_KSCF,
    -- FACTORS
    agg.PRESSURE AS PRESSURE_FACTOR,
    agg.GAS_FORMATION_VOLUME_FACTOR AS GAS_FORMATION_FACTOR,
    agg.WATER_FORMATION_VOLUME_FACTOR AS WATER_FORMATION_FACTOR,
    agg.SOLUTION_GAS_OIL_RATIO,
    agg.VOLATILIZED_OIL_GAS_RATIO,
    agg.VISCOSITY_OIL,
    agg.VISCOSITY_GAS,
    agg.VISCOSITY_WATER,
    agg.INJECTED_GAS_FORMATION_VOLUME_FACTOR AS INJECTED_GAS_FACTOR,
    agg.INJECTED_WATER_FORMATION_VOLUME_FACTOR AS INJECTED_WATER_FACTOR,
    agg.FACTOR AS FACTORS
FROM Aggregated_Data agg
WHERE agg.CUMULATIVE_PRODUCTION_VOLUME_res_bbl > 0
GROUP BY 
    agg.ID_PATTERN, 
    agg.DATE, 
    agg.PRESSURE,
    agg.CUMULATIVE_INJECTION_VOLUME_res_bbl,
    agg.CUMULATIVE_PRODUCTION_VOLUME_res_bbl,
    agg.THEOR_OIL_VOL_STB,
    agg.THEOR_WATER_VOL_STB,
    agg.THEOR_GAS_VOL_KSCF,
    agg.GAS_FORMATION_VOLUME_FACTOR,
    agg.WATER_FORMATION_VOLUME_FACTOR,
    agg.THEOR_WATER_INJ_VOL_STB,
    agg.INJECTED_WATER_FORMATION_VOLUME_FACTOR,
    agg.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
    agg.SOLUTION_GAS_OIL_RATIO,
    agg.VOLATILIZED_OIL_GAS_RATIO,
    agg.VISCOSITY_OIL,
    agg.VISCOSITY_GAS,
    agg.VISCOSITY_WATER,
    agg.FACTOR;
