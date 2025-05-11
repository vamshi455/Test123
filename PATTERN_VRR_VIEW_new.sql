-- Create view for PATTERN_VRR_VIEW with Snowflake mappings
CREATE OR REPLACE VIEW RMDE_SAM_ACC.PATTERN_VRR_VIEW
AS
SELECT *
FROM (
    SELECT
        CONCAT(ID_PATTERN, TO_CHAR(DATE, 'DD-MM-YYYY')) AS ID_PATTERN_VRR,
        ID_PATTERN,
        DATE,
        PRESSURE,
        COALESCE((INJECTION_VOLUME_RES_BBL / NULLIF(PRODUCTION_VOLUME_RES_BBL, 0)), 0) AS VRR,
        COALESCE((CUMULATIVE_INJECTION_VOLUME_RES_BBL / NULLIF(CUMULATIVE_PRODUCTION_VOLUME_RES_BBL, 0)), 0) AS CUMULATIVE_VRR,
        OIL_VOLUME_STB,
        OIL_VOLUME_RES_BBL,
        GAS_VOLUME_SCF,
        FREE_GAS,
        WATER_VOLUME_STB,
        WATER_VOLUME_RES_BBL,
        PRODUCTION_VOLUME_RES_BBL,
        CUMULATIVE_PRODUCTION_VOLUME_RES_BBL,
        GAS_INJ_VOLUME_SCF,
        WATER_INJ_VOLUME_STB,
        INJECTION_VOLUME_RES_BBL,
        CUMULATIVE_INJECTION_VOLUME_RES_BBL,
        WATER_INJ_VOLUME_RES_BBL,
        GAS_INJ_VOLUME_RES_BBL,
        CUMULATIVE_OIL_PRODUCTION_VOLUME_RES_BBL,
        CUMULATIVE_WATER_PRODUCTION_VOLUME_RES_BBL,
        CUMULATIVE_WATER_INJECTION_VOLUME_RES_BBL,
        CUMULATIVE_GAS_INJECTION_VOLUME_RES_BBL
    FROM (
        SELECT
            ID_PATTERN,
            DATE,
            OIL_VOLUME_STB,
            WATER_VOLUME_STB,
            OIL_VOLUME_RES_BBL,
            WATER_VOLUME_RES_BBL,
            WATER_INJ_VOLUME_STB,
            WATER_INJ_VOLUME_RES_BBL,
            GAS_WELL_GAS_VOLUME_SCF,
            GAS_INJ_VOLUME_SCF,
            GAS_VOLUME_SCF,
            FREE_GAS,
            SUM(OIL_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_OIL_PRODUCTION_VOLUME_RES_BBL,
            SUM(WATER_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_WATER_PRODUCTION_VOLUME_RES_BBL,
            SUM(WATER_INJ_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_WATER_INJECTION_VOLUME_RES_BBL,
            SUM(GAS_INJ_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_GAS_INJECTION_VOLUME_RES_BBL,
            GAS_INJ_VOLUME_RES_BBL,
            PRODUCTION_VOLUME_RES_BBL,
            SUM(OIL_VOLUME_RES_BBL + WATER_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_PRODUCTION_VOLUME_RES_BBL,
            INJECTION_VOLUME_RES_BBL,
            SUM(INJECTION_VOLUME_RES_BBL) OVER (PARTITION BY ID_PATTERN ORDER BY DATE ASC) AS CUMULATIVE_INJECTION_VOLUME_RES_BBL,
            SOLUTION_GAS_OIL_RATIO,
            PRESSURE
        FROM (
            SELECT
                ID_PATTERN,
                DATE,
                SUM(OIL_VOLUME) AS OIL_VOLUME_STB,
                SUM(WATER_VOLUME) AS WATER_VOLUME_STB,
                SUM(OIL_VOLUME * OIL_FORMATION_VOLUME_FACTOR) AS OIL_VOLUME_RES_BBL,
                SUM(WATER_VOLUME * WATER_FORMATION_VOLUME_FACTOR) AS WATER_VOLUME_RES_BBL,
                SUM(WATER_INJ_VOLUME) AS WATER_INJ_VOLUME_STB,
                SUM(WATER_INJ_VOLUME * INJECTED_WATER_FORMATION_VOLUME_FACTOR) AS WATER_INJ_VOLUME_RES_BBL,
                SUM(GAS_WELL_GAS_VOLUME) AS GAS_WELL_GAS_VOLUME_SCF,
                SUM(GAS_INJ_VOLUME) AS GAS_INJ_VOLUME_SCF,
                SUM(GAS_VOLUME) AS GAS_VOLUME_SCF,
                SUM(FREE_GAS) AS FREE_GAS,
                SUM(GAS_INJ_VOLUME * INJECTED_GAS_FORMATION_VOLUME_FACTOR) AS GAS_INJ_VOLUME_RES_BBL,
                SUM(OIL_VOLUME * OIL_FORMATION_VOLUME_FACTOR) + SUM(WATER_VOLUME * WATER_FORMATION_VOLUME_FACTOR) + SUM(FREE_GAS) AS PRODUCTION_VOLUME_RES_BBL,
                SUM(WATER_INJ_VOLUME * INJECTED_WATER_FORMATION_VOLUME_FACTOR) + SUM(GAS_INJ_VOLUME * INJECTED_GAS_FORMATION_VOLUME_FACTOR) AS INJECTION_VOLUME_RES_BBL,
                SUM(SOLUTION_GAS_OIL_RATIO) AS SOLUTION_GAS_OIL_RATIO,
                AVG(PRESSURE) AS PRESSURE
            FROM (
                SELECT
                    daily_volume.ID_PATTERN,
                    daily_volume.COMPLETION_ID AS ID_COMPLETION,
                    daily_volume.PROD_DATE AS DATE,
                    COALESCE(daily_volume.THEOR_OIL_VOL_STB * split_factors.FACTOR, daily_volume.THEOR_OIL_VOL_STB, 0) AS OIL_VOLUME,
                    COALESCE(daily_volume.THEOR_WATER_VOL_STB * split_factors.FACTOR, daily_volume.THEOR_WATER_VOL_STB, 0) AS WATER_VOLUME,
                    COALESCE(daily_volume.THEOR_GAS_VOL_KSCF * 1000 * split_factors.FACTOR, daily_volume.THEOR_GAS_VOL_KSCF * 1000, 0) AS GAS_VOLUME,
                    COALESCE(daily_volume.THEOR_WATER_INJ_VOL_STB * split_factors.FACTOR, daily_volume.THEOR_WATER_INJ_VOL_STB, 0) AS WATER_INJ_VOLUME,
                    COALESCE(daily_volume.ALLOC_GAS_VOL_KSCF * 1000 * split_factors.FACTOR, daily_volume.ALLOC_GAS_VOL_KSCF * 1000, 0) AS GAS_WELL_GAS_VOLUME,
                    COALESCE(daily_volume.THEOR_GAS_INJ_VOL_KSCF * 1000 * split_factors.FACTOR, daily_volume.THEOR_GAS_INJ_VOL_KSCF * 1000, 0) AS GAS_INJ_VOLUME,
                    COALESCE(
                        (daily_volume.THEOR_GAS_VOL_KSCF * 1000 / NULLIF(daily_volume.THEOR_OIL_VOL_STB, 0) - pvt.SOLUTION_GAS_OIL_RATIO) * daily_volume.THEOR_OIL_VOL_STB * split_factors.FACTOR * pvt.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                        0
                    ) AS FREE_GAS,
                    split_factors.FACTOR,
                    add_pressures.PRESSURE,
                    pvt.OIL_FORMATION_VOLUME_FACTOR,
                    pvt.GAS_FORMATION_VOLUME_FACTOR,
                    pvt.WATER_FORMATION_VOLUME_FACTOR,
                    pvt.SOLUTION_GAS_OIL_RATIO,
                    pvt.VOLATIZED_OIL_GAS_RATIO,
                    pvt.VISCOSITY_OIL,
                    pvt.VISCOSITY_WATER,
                    pvt.VISCOSITY_GAS,
                    pvt.INJECTED_GAS_FORMATION_VOLUME_FACTOR,
                    pvt.INJECTED_WATER_FORMATION_VOLUME_FACTOR
                FROM TRUSTED_DB.PRODUCTION_VOLUME.PRODUCTION_VOLUMES_DAILY_OILFIELD daily_volume
                LEFT JOIN RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR split_factors
                    ON daily_volume.COMPLETION_ID = split_factors.ID_COMPLETION
                    AND split_factors.EFFECT_DATE = (
                        SELECT MAX(EFFECT_DATE)
                        FROM RMDE_SAM_ACC.PATTERN_CONTRIBUTION_FACTOR
                        WHERE ID_PATTERN = split_factors.ID_PATTERN
                          AND ID_COMPLETION = daily_volume.COMPLETION_ID }{\
                          AND daily_volume.PROD_DATE >= EFFECT_DATE
                    )
                INNER JOIN (
                    SELECT
                        ID_PATTERN,
                        DATE,
                        PRESSURE,
                        COALESCE(
                            LEAD(DATE, 1) OVER (PARTITION BY ID_PATTERN ORDER BY DATE),
                            '9999-12-31'::DATE
                        ) AS END_DATE
                    FROM RMDE_SAM_ACC.PATTERN_PRESSURE
                ) add_pressures
                    ON add_pressures.ID_PATTERN = split_factors.ID_PATTERN
                    AND daily_volume.PROD_DATE >= add_pressures.DATE
                    AND daily_volume.PROD_DATE < add_pressures.END_DATE
                LEFT JOIN RMDE_SAM_ACC.INTERPOLATE_PVT_COMPLETION_TEST_VIEW pvt
                    ON pvt.ID_COMPLETION = daily_volume.COMPLETION_ID
                    AND pvt.VRR_DATE = add_pressures.DATE
                    AND pvt.PRESSURE = add_pressures.PRESSURE
                WHERE daily_volume.THEOR_GAS_VOL_KSCF IS NOT NULL
            ) splits
            GROUP BY ID_PATTERN, DATE, PRESSURE
        ) aggregated
    ) final
) nonzeroes
WHERE CUMULATIVE_PRODUCTION_VOLUME_RES_BBL != 0;
