-- Snowflake does not require SET statements like ANSI_NULLS or QUOTED_IDENTIFIER
-- VRR CALCULATIONS (OILFIELD STANDARD) --

CREATE OR REPLACE VIEW vr.PATTERN_VRR_VIEW
AS
SELECT
    CONCAT(ID_PATTERN, TO_CHAR(DATE, 'DD-MON-YYYY')) AS ID_PATTERN,  -- FORMAT in T-SQL replaced with TO_CHAR in Snowflake
    DATE,
    PRESSURE,
    COALESCE(CUMULATIVE_INJECTION_VOLUME_res_bbl / NULLIF(CUMULATIVE_PRODUCTION_VOLUME_res_bbl, 0), 0) AS VRR_Injection_Volume_Production_Volume,  -- Adjusted column naming for clarity and Snowflake compatibility
    -- OIL VOLUME (stb)
    OIL_VOLUME_stb,
    -- FREE GAS
    WATER_VOLUME_stb,
    PRODUCTION_VOLUME_res_bbl,
    GAS_FORMATION_VOLUME_FACTOR,
    WATER_FORMATION_VOLUME_FACTOR,
    GAS_INJ_VOLUME_stb,
    WATER_INJ_VOLUME_stb,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR AS WATER_INJ_VOLUME,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR AS GAS_WELL_GAS_VOLUME,
    GAS_WELL_GAS_VOLUME_scf,
    -- CASE statement for FREE_GAS is commented out in the original; leaving it as-is
    -- FACTORS
    PRESSURE AS PRESSURE_FACTOR,  -- Renamed to avoid duplicate column names
    GAS_FORMATION_VOLUME_FACTOR AS GAS_FORMATION_FACTOR,
    WATER_FORMATION_VOLUME_FACTOR AS WATER_FORMATION_FACTOR,
    SOLUTION_GAS_OIL_RATIO,
    VOLATILIZED_OIL_GAS_RATIO,
    VISCOSITY_OIL,
    VISCOSITY_GAS,
    VISCOSITY_WATER,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR AS INJECTED_GAS_FACTOR,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR AS INJECTED_WATER_FACTOR,
    -- [Amount Type] -- added 08/29/24
    FACTORS
FROM (
    SELECT
        -- Raw Daily Volume Data for Injectors and Producers
        -- Filter out any pattern without factors
        split_factors.ID_PATTERN AS ID_PATTERN,
        daily_volume.Operating_Date AS DATE,
        add_pressures.PRESSURE AS PRESSURE_DATE,
        COALESCE(daily_volume.Oil_Volume_stb, 0) AS OIL_VOLUME_stb,
        COALESCE(daily_volume.Water_Prod_Volume_stb, 0) AS WATER_VOLUME_stb,
        COALESCE(daily_volume.Gas_Prod_Volume_kscf * 1000, 0) AS GAS_WELL_GAS_VOLUME_scf,  -- kscf to scf
        split_factors.*,
        -- Split Factors will vary with time. Match with the split_factors with the most recent 'effective_date'
        daily_volume.ID_Completion AS split_factors_ID_PATTERN,
        daily_volume.Operating_Date AS EFFECT_DATE
    FROM (
        SELECT
            ID_PATTERN,
            ID_Completion,
            Operating_Date,
            Oil_Volume_stb,
            Water_Prod_Volume_stb,
            Gas_Prod_Volume_kscf,
            CUMULATIVE_INJECTION_VOLUME_res_bbl,
            CUMULATIVE_PRODUCTION_VOLUME_res_bbl
        FROM daily_volume
        WHERE ID_Completion IN (
            SELECT
                ID_Completion
            FROM (
                SELECT
                    ID_PATTERN,
                    DATE,
                    COALESCE(
                        LEAD(DATE, 1) OVER (PARTITION BY ID_PATTERN ORDER BY DATE),
                        TO_TIMESTAMP('9999-12-31 00:00:00')
                    ) AS END_DATE
                FROM vr.PATTERN_PRESSURE
            ) sub
            JOIN add_pressures 
                ON sub.ID_PATTERN = add_pressures.ID_PATTERN
                AND daily_volume.Operating_Date >= add_pressures.DATE
                AND daily_volume.Operating_Date < sub.END_DATE
        )
    ) daily_volume
    LEFT JOIN split_factors 
        ON split_factors.ID_PATTERN = daily_volume.ID_PATTERN
        AND daily_volume.Operating_Date <= (
            SELECT MAX(EFFECT_DATE) 
            FROM vr.PATTERN_CONTRIBUTION_FACTOR 
            WHERE split_factors.ID_PATTERN = ID_PATTERN 
            AND daily_volume.Operating_Date <= EFFECT_DATE
        )
    LEFT JOIN add_pressures 
        ON daily_volume.ID_PATTERN = add_pressures.ID_PATTERN
        AND daily_volume.Operating_Date >= add_pressures.DATE
        -- Match the pressure to the volume
    LEFT JOIN (
        -- Replace CROSS APPLY with a subquery or LEFT JOIN
        -- Assuming vr.InterpolateCompletionPressure is a function that can be rewritten or exists in Snowflake
        SELECT
            ID_Completion,
            DATE,
            vr.InterpolateCompletionPressure(ID_Completion, DATE) AS PRESSURE
        FROM daily_volume
    ) interpolated_pressure
        ON daily_volume.ID_Completion = interpolated_pressure.ID_Completion
        AND daily_volume.Operating_Date = interpolated_pressure.DATE
) aggregates
GROUP BY ID_PATTERN, DATE, PRESSURE
WHERE CUMULATIVE_PRODUCTION_VOLUME_res_bbl > 0;
