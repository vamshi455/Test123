SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Author: <Author,,Name>
-- Create date: <Create Date,,>
-- Description: <Description,,>

ALTER FUNCTION [vrr].[InterpolatePVT[CompletionTest]]
(
    @pressure float,
    @completion varchar(32),
    @vrr_date date
)
RETURNS
@RoundedInterpolatedValues TABLE
(
    PRESSURE float,
    OIL_FORMATION_VOLUME_FACTOR float,
    GAS_FORMATION_VOLUME_FACTOR float,
    WATER_FORMATION_VOLUME_FACTOR float,
    SOLUTION_GAS_OIL_RATIO float,
    VOLATIZED_OIL_GAS_RATIO float,
    VISCOSITY_OIL float,
    VISCOSITY_WATER float,
    VISCOSITY_GAS float,
    INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
    INJECTED_WATER_FORMATION_VOLUME_FACTOR float
)
AS
BEGIN
    -- Fill the table variable with the rows for your result set
    DECLARE @ExactMatch TABLE (
        PRESSURE float,
        OIL_FORMATION_VOLUME_FACTOR float,
        GAS_FORMATION_VOLUME_FACTOR float,
        WATER_FORMATION_VOLUME_FACTOR float,
        SOLUTION_GAS_OIL_RATIO float,
        VOLATIZED_OIL_GAS_RATIO float,
        VISCOSITY_OIL float,
        VISCOSITY_WATER float,
        VISCOSITY_GAS float,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR float
    )

    DECLARE @lowerbound TABLE (
        PRESSURE float,
        OIL_FORMATION_VOLUME_FACTOR float,
        GAS_FORMATION_VOLUME_FACTOR float,
        WATER_FORMATION_VOLUME_FACTOR float,
        SOLUTION_GAS_OIL_RATIO float,
        VOLATIZED_OIL_GAS_RATIO float,
        VISCOSITY_OIL float,
        VISCOSITY_WATER float,
        VISCOSITY_GAS float,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR float
    )

    DECLARE @upperbound TABLE (
        PRESSURE float,
        OIL_FORMATION_VOLUME_FACTOR float,
        GAS_FORMATION_VOLUME_FACTOR float,
        WATER_FORMATION_VOLUME_FACTOR float,
        SOLUTION_GAS_OIL_RATIO float,
        VOLATIZED_OIL_GAS_RATIO float,
        VISCOSITY_OIL float,
        VISCOSITY_WATER float,
        VISCOSITY_GAS float,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR float
    )

    DECLARE @interpolatedValues TABLE (
        PRESSURE float,
        OIL_FORMATION_VOLUME_FACTOR float,
        GAS_FORMATION_VOLUME_FACTOR float,
        WATER_FORMATION_VOLUME_FACTOR float,
        SOLUTION_GAS_OIL_RATIO float,
        VOLATIZED_OIL_GAS_RATIO float,
        VISCOSITY_OIL float,
        VISCOSITY_WATER float,
        VISCOSITY_GAS float,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR float
    )

    DECLARE @PVTwithEndDate TABLE (
        ID_COMPLETION varchar(32),
        TEST_DATE date,
        PRESSURE float,
        OIL_FORMATION_VOLUME_FACTOR float,
        GAS_FORMATION_VOLUME_FACTOR float,
        WATER_FORMATION_VOLUME_FACTOR float,
        SOLUTION_GAS_OIL_RATIO float,
        VOLATIZED_OIL_GAS_RATIO float,
        VISCOSITY_OIL float,
        VISCOSITY_WATER float,
        VISCOSITY_GAS float,
        INJECTED_GAS_FORMATION_VOLUME_FACTOR float,
        INJECTED_WATER_FORMATION_VOLUME_FACTOR float,
        END_DATE date
    )

    INSERT INTO @PVTwithEndDate (
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
        END_DATE
    )
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
            (SELECT min(TEST_DATE)
             FROM vrr.COMPLETION_PVT_CHARACTERISTICS cpvt
             WHERE cpvt.TEST_DATE > vrr.COMPLETION_PVT_CHARACTERISTICS.TEST_DATE
               AND cpvt.ID_COMPLETION = vrr.COMPLETION_PVT_CHARACTERISTICS.ID_COMPLETION),
            '9999-12-31 00:00:00.0000000'
        ) AS END_DATE
    FROM vrr.COMPLETION_PVT_CHARACTERISTICS
    WHERE ID_COMPLETION = @completion
      AND TEST_DATE <= @EOMONTH(@vrr_date)
      AND TEST_DATE >= @pressure
    ORDER BY TEST_DATE DESC

    -- exact match
    INSERT INTO @ExactMatch (
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
    )
    SELECT TOP 1
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
    FROM @PVTwithEndDate
    WHERE TEST_DATE = (
        SELECT max(TEST_DATE)
        FROM @PVTwithEndDate p
        WHERE p.PRESSURE = @pressure
          AND p.ID_COMPLETION = @completion
          AND p.TEST_DATE <= @EOMONTH(@vrr_date)
    )
      AND PRESSURE = @pressure
    ORDER BY TEST_DATE DESC

    -- Lowerbound, whose pressure value are less than the passed in pressure, but as close to it as possible
    INSERT INTO @lowerbound (
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
    )
    SELECT TOP 1
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
    FROM @PVTwithEndDate
    WHERE PRESSURE < @pressure
      AND ID_COMPLETION = @completion
      AND TEST_DATE <= @EOMONTH(@vrr_date)
    ORDER BY PRESSURE DESC, TEST_DATE DESC

    -- If InterpolatedValues is empty, means no exact match, and no interpolation/extrapolation was done
    -- that value and return it
    IF (
        (SELECT count(*) FROM @lowerbound) = 1
        AND (SELECT count(*) FROM @upperbound) = 1
        AND (SELECT count(*) FROM @interpolatedValues) = 0
    )
    BEGIN
        INSERT INTO @interpolatedValues
        SELECT
            @pressure,
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT OIL_FORMATION_VOLUME_FACTOR FROM @lowerbound),
                (SELECT OIL_FORMATION_VOLUME_FACTOR FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT GAS_FORMATION_VOLUME_FACTOR FROM @lowerbound),
                (SELECT GAS_FORMATION_VOLUME_FACTOR FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT WATER_FORMATION_VOLUME_FACTOR FROM @lowerbound),
                (SELECT WATER_FORMATION_VOLUME_FACTOR FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT SOLUTION_GAS_OIL_RATIO FROM @lowerbound),
                (SELECT SOLUTION_GAS_OIL_RATIO FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT VOLATIZED_OIL_GAS_RATIO FROM @lowerbound),
                (SELECT VOLATIZED_OIL_GAS_RATIO FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT VISCOSITY_OIL FROM @lowerbound),
                (SELECT VISCOSITY_OIL FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT VISCOSITY_WATER FROM @lowerbound),
                (SELECT VISCOSITY_WATER FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT VISCOSITY_GAS FROM @lowerbound),
                (SELECT VISCOSITY_GAS FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM @lowerbound),
                (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM @upperbound)
            ),
            vrr.Extrapolate(
                (SELECT PRESSURE FROM @lowerbound),
                (SELECT PRESSURE FROM @upperbound),
                (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM @lowerbound),
                (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM @upperbound)
            )
    END

    -- If there is a upper bound, but no lower bound
    IF (
        (SELECT count(*) FROM @upperbound) = 1
        AND (SELECT count(*) FROM @lowerbound) = 0
        AND (SELECT count(*) FROM @ExactMatch) = 0
        AND (SELECT count(*) FROM @PVTwithEndDate WHERE ID_COMPLETION = @completion AND PRESSURE > @pressure AND TEST_DATE <= @EOMONTH(@vrr_date)) > 1
    )
    BEGIN
        INSERT INTO @secondBOUND (
            PRESSURE,
            OIL_FORMATION_VOLUME_FACTOR,
            GAS_FORMATION_VOLUME_FACTOR,
            WATER_FORMATION_VOLUME_FACTOR,
            VISCOSITY_GAS_OIL_RATIO,
            VOLATIZED_OIL_GAS_RATIO,
            VISCOSITY_OIL,
            VISCOSITY_WATER,
            VISCOSITY_GAS,
            INJECTED_GAS_FORMATION_VOLUME_FACTOR,
            INJECTED_WATER_FORMATION_VOLUME_FACTOR
        )
        SELECT TOP 1
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
        FROM @PVTwithEndDate pvt
        WHERE pvt.PRESSURE = (
            SELECT min(pressure)
            FROM @PVTwithEndDate p
            WHERE p.PRESSURE > (SELECT PRESSURE FROM @upperbound)
              AND p.ID_COMPLETION = @completion
              AND p.TEST_DATE <= @EOMONTH(@vrr_date)
        )
        ORDER BY TEST_DATE DESC

        -- If there is a exact match, lets put that in InterpolatedValues, which will be returned
        IF (
            (SELECT count(*) FROM @ExactMatch) = 1
        )
        BEGIN
            INSERT INTO @interpolatedValues
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
            FROM @ExactMatch
        END

        -- If there is no exact match, just run an interpolation with these values
        IF (
            (SELECT count(*) FROM @upperbound) = 1
            AND (SELECT count(*) FROM @secondBOUND) = 1
            AND (SELECT count(*) FROM @ExactMatch) = 0
        )
        BEGIN
            INSERT INTO @interpolatedValues
            SELECT
                @pressure,
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT OIL_FORMATION_VOLUME_FACTOR FROM @upperbound),
                    (SELECT OIL_FORMATION_VOLUME_FACTOR FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT GAS_FORMATION_VOLUME_FACTOR FROM @upperbound),
                    (SELECT GAS_FORMATION_VOLUME_FACTOR FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT WATER_FORMATION_VOLUME_FACTOR FROM @upperbound),
                    (SELECT WATER_FORMATION_VOLUME_FACTOR FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT SOLUTION_GAS_OIL_RATIO FROM @upperbound),
                    (SELECT SOLUTION_GAS_OIL_RATIO FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT VOLATIZED_OIL_GAS_RATIO FROM @upperbound),
                    (SELECT VOLATIZED_OIL_GAS_RATIO FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT VISCOSITY_OIL FROM @upperbound),
                    (SELECT VISCOSITY_OIL FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT VISCOSITY_WATER FROM @upperbound),
                    (SELECT VISCOSITY_WATER FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT VISCOSITY_GAS FROM @upperbound),
                    (SELECT VISCOSITY_GAS FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM @upperbound),
                    (SELECT INJECTED_GAS_FORMATION_VOLUME_FACTOR FROM @secondBOUND)
                ),
                vrr.Extrapolate(
                    (SELECT PRESSURE FROM @upperbound),
                    (SELECT PRESSURE FROM @secondBOUND),
                    (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM @upperbound),
                    (SELECT INJECTED_WATER_FORMATION_VOLUME_FACTOR FROM @secondBOUND)
                )
        END

        -- No active values of the given completion
        IF (
            (SELECT count(*) FROM @interpolatedValues) = 0
        )
        BEGIN
            INSERT INTO @interpolatedValues
            SELECT
                @pressure,
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
        END

        INSERT INTO @RoundedInterpolatedValues
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
        FROM @interpolatedValues
    END
    RETURN
END
