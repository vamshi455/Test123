CREATE OR REPLACE FUNCTION vrr.InterpolatePVTCompletionTest (
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
DECLARE
    ExactMatch RESULTSET;
    lowerbound RESULTSET;
    upperbound RESULTSET;
    interpolatedValues RESULTSET;
    secondBOUND RESULTSET;
    PVTwithEndDate RESULTSET;
BEGIN
    -- Populate PVTwithEndDate
    PVTwithEndDate := (
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
                 FROM vrr.COMPLETION_PVT_CHARACTERISTICS cpvt
                 WHERE cpvt.TEST_DATE > cpc.TEST_DATE
                   AND cpvt.ID_COMPLETION = cpc.ID_COMPLETION),
                '9999-12-31'
            ) AS END_DATE
        FROM vrr.COMPLETION_PVT_CHARACTERISTICS cpc
        WHERE ID_COMPLETION = completion
          AND TEST_DATE <= LAST_DAY(vrr_date)
          AND TEST_DATE >= pressure
        ORDER BY TEST_DATE DESC
    );

    -- Exact match
    ExactMatch := (
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
        FROM :PVTwithEndDate
        WHERE TEST_DATE = (
            SELECT MAX(TEST_DATE)
            FROM :PVTwithEndDate p
            WHERE p.PRESSURE = pressure
              AND p.ID_COMPLETION = completion
              AND p.TEST_DATE <= LAST_DAY(vrr_date)
        )
          AND PRESSURE = pressure
        ORDER BY TEST_DATE DESC
        LIMIT 1
    );

    -- Lowerbound
    lowerbound := (
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
        FROM :PVTwithEndDate
        WHERE PRESSURE < pressure
          AND ID_COMPLETION = completion
          AND TEST_DATE <= LAST_DAY(vrr_date)
        ORDER BY PRESSURE DESC, TEST_DATE DESC
        LIMIT 1
    );

    -- Upperbound
    upperbound := (
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
        FROM :PVTwithEndDate
        WHERE PRESSURE > pressure
          AND ID_COMPLETION = completion
          AND TEST_DATE <= LAST_DAY(vrr_date)
        ORDER BY PRESSURE ASC, TEST_DATE DESC
        LIMIT 1
    );

    -- Interpolation logic
    IF ((SELECT COUNT(*) FROM :lowerbound) = 1 AND (SELECT COUNT(*) FROM :upperbound) = 1 AND (SELECT COUNT(*) FROM :interpolatedValues) = 0) THEN
        interpolatedValues := (
            SELECT
                pressure,
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT OIL_FORMATION_VOLUME_FACTOR FROM :lowerbound),
                    (SELECT OIL_FORMATION_VOLUME_FACTOR FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT GAS_FORMATION_VOLUME_FACTOR FROM :lowerbound),
                    (SELECT GAS_FORMATION_VOLUME_FACTOR FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT WATER_FORMATION_VOLUME_FACTOR FROM :lowerbound),
                    (SELECT WATER_FORMATION_VOLUME_FACTOR FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT SOLUTION_GAS_OIL_RATIO FROM :lowerbound),
                    (SELECT SOLUTION_GAS_OIL_RATIO FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT VOLATIZED_OIL_GAS_RATIO FROM :lowerbound),
                    (SELECT VOLATIZED_OIL_GAS_RATIO FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT VISCOSITY_OIL FROM :lowerbound),
                    (SELECT VISCOSITY_OIL FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT VISCOSITY_WATER FROM :lowerbound),
                    (SELECT VISCOSITY_WATER FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT VISCOSITY_GAS FROM :lowerbound),
                    (SELECT VISCOSITY_GAS FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM :lowerbound),
                    (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM :upperbound)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM :lowerbound),
                    (SELECT PRESSURE FROM :upperbound),
                    (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM :lowerbound),
                    (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM :upperbound)
                )
        );
    END IF;

    -- Second bound for no lower bound case
    IF ((SELECT COUNT(*) FROM :upperbound) = 1 AND (SELECT COUNT(*) FROM :lowerbound) = 0 AND (SELECT COUNT(*) FROM :ExactMatch) = 0
        AND (SELECT COUNT(*) FROM :PVTwithEndDate WHERE ID_COMPLETION = completion AND PRESSURE > pressure AND TEST_DATE <= LAST_DAY(vrr_date)) > 1) THEN
        secondBOUND := (
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
            FROM :PVTwithEndDate pvt
            WHERE pvt.PRESSURE = (
                SELECT MIN(pressure)
                FROM :PVTwithEndDate p
                WHERE p.PRESSURE > (SELECT PRESSURE FROM :upperbound)
                  AND p.ID_COMPLETION = completion
                  AND p.TEST_DATE <= LAST_DAY(vrr_date)
            )
            ORDER BY TEST_DATE DESC
            LIMIT 1
        );

        -- Exact match case
        IF ((SELECT COUNT(*) FROM :ExactMatch) = 1) THEN
            interpolatedValues := (
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
                FROM :ExactMatch
            );
        END IF;

        -- Interpolation with upper and second bound
        IF ((SELECT COUNT(*) FROM :upperbound) = 1 AND (SELECT COUNT(*) FROM :secondBOUND) = 1 AND (SELECT COUNT(*) FROM :ExactMatch) = 0) THEN
            interpolatedValues := (
                SELECT
                    pressure,
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM :upperbound),
                        (SELECT OIL_FORMATION_VOLUME_FACTOR FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM :upperbound),
                        (SELECT GAS_FORMATION_VOLUME_FACTOR FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM :upperbound),
                        (SELECT WATER_FORMATION_VOLUME_FACTOR FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM :upperbound),
                        (SELECT SOLUTION_GAS_OIL_RATIO FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT VOLATIZED_OIL_GAS_RATIO FROM :upperbound),
                        (SELECT VOLATIZED_OIL_GAS_RATIO FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT VISCOSITY_OIL FROM :upperbound),
                        (SELECT VISCOSITY_OIL FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT VISCOSITY_WATER FROM :upperbound),
                        (SELECT VISCOSITY_WATER FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT VISCOSITY_GAS FROM :upperbound),
                        (SELECT VISCOSITY_GAS FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM :upperbound),
                        (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM :secondBOUND)
                    ),
                    vrr.Extrapolate(
                        (SELECT PRESSURE FROM :upperbound),
                        (SELECT PRESSURE FROM :secondBOUND),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM :upperbound),
                        (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM :secondBOUND)
                    )
            );
        END IF;
    END IF;

    -- No active values case
    IF ((SELECT COUNT(*) FROM :interpolatedValues) = 0) THEN
        interpolatedValues := (
            SELECT
                pressure,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
        );
    END IF;

    -- Return rounded results
    RETURN (
        SELECT
            ROUND(PRESSURE, 5),
            ROUND(OIL_FORMATION_VOLUME_FACTOR, 5),
            ROUND(GAS_FORMATION_VOLUME_FACTOR, 5),
            ROUND(WATER_FORMATION_VOLUME_FACTOR, 5),
            ROUND(SOLUTION_GAS_OIL_RATIO, 5),
            ROUND(VOLATIZED_OIL_GAS_RATIO, 5),
            ROUND(VISCOSITY_OIL, 5),
            ROUND(VISCOSITY_WATER, 5),
            ROUND(VISCOSITY_GAS, 5),
            ROUND(INJECTED_GAS_FORMATION_VOLUME_FACTOR, 5),
            ROUND(INJECTED_WATER_FORMATION_VOLUME_FACTOR, 5)
        FROM :interpolatedValues
    );
END;
$$
;
