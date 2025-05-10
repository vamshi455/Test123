-- Create or replace the InterpolatePVTCompletionTest function
CREATE OR REPLACE FUNCTION RMDE_SAM_ACC.InterpolatePVTCompletionTest(
    pressure FLOAT,
    completion VARCHAR,
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
LANGUAGE JAVASCRIPT
AS
$$
    // Declare all variables at the top
    var lastDayStmt;
    var lastDayResult;
    var lastDay;
    var pvtQuery;
    var pvtStmt;
    var pvtResults;
    var pvtData = [];
    var exactMatch = null;
    var lowerBound = null;
    var upperBound = null;
    var secondBound = null;
    var interpolatedValues = [];
    var i;
    var row;
    var minPressure;
    var matchingRows;
    var result;

    // Compute LAST_DAY of vrr_date
    lastDayStmt = snowflake.createStatement({
        sqlText: 'SELECT LAST_DAY(:1)',
        binds: [vrr_date]
    });
    lastDayResult = lastDayStmt.execute();
    if (!lastDayResult.next()) {
        return [{
            PRESSURE: pressure,
            OIL_FORMATION_VOLUME_FACTOR: null,
            GAS_FORMATION_VOLUME_FACTOR: null,
            WATER_FORMATION_VOLUME_FACTOR: null,
            SOLUTION_GAS_OIL_RATIO: null,
            VOLATIZED_OIL_GAS_RATIO: null,
            VISCOSITY_OIL: null,
            VISCOSITY_WATER: null,
            VISCOSITY_GAS: null,
            INJECTED_GAS_FORMATION_VOLUME_FACTOR: null,
            INJECTED_WATER_FORMATION_VOLUME_FACTOR: null
        }];
    }
    lastDay = lastDayResult.getColumnValue(1);

    // Query to get PVT data
    pvtQuery = `
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
            INJECTED_WATER_FORMATION_VOLUME_FACTOR
        FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS
        WHERE ID_COMPLETION = :1
          AND TEST_DATE <= :2
        ORDER BY TEST_DATE DESC
    `;

    pvtStmt = snowflake.createStatement({
        sqlText: pvtQuery,
        binds: [completion, lastDay]
    });

    pvtResults = pvtStmt.execute();
    while (pvtResults.next()) {
        row = {
            id_completion: pvtResults.getColumnValue('ID_COMPLETION'),
            test_date: pvtResults.getColumnValue('TEST_DATE'),
            pressure: pvtResults.getColumnValue('PRESSURE'),
            oil_formation_volume_factor: pvtResults.getColumnValue('OIL_FORMATION_VOLUME_FACTOR'),
            gas_formation_volume_factor: pvtResults.getColumnValue('GAS_FORMATION_VOLUME_FACTOR'),
            water_formation_volume_factor: pvtResults.getColumnValue('WATER_FORMATION_VOLUME_FACTOR'),
            solution_gas_oil_ratio: pvtResults.getColumnValue('SOLUTION_GAS_OIL_RATIO'),
            volatized_oil_gas_ratio: pvtResults.getColumnValue('VOLATIZED_OIL_GAS_RATIO'),
            viscosity_oil: pvtResults.getColumnValue('VISCOSITY_OIL'),
            viscosity_water: pvtResults.getColumnValue('VISCOSITY_WATER'),
            viscosity_gas: pvtResults.getColumnValue('VISCOSITY_GAS'),
            injected_gas_formation_volume_factor: pvtResults.getColumnValue('INJECTED_GAS_FORMATION_VOLUME_FACTOR'),
            injected_water_formation_volume_factor: pvtResults.getColumnValue('INJECTED_WATER_FORMATION_VOLUME_FACTOR')
        };
        pvtData.push(row);
    }

    // Find exact match
    for (i = 0; i < pvtData.length; i++) {
        row = pvtData[i];
        if (row.pressure === pressure && 
            row.id_completion === completion && 
            new Date(row.test_date) <= new Date(lastDay)) {
            exactMatch = row;
            break;
        }
    }

    // Find lower bound
    for (i = 0; i < pvtData.length; i++) {
        row = pvtData[i];
        if (row.pressure < pressure && 
            row.id_completion === completion && 
            new Date(row.test_date) <= new Date(lastDay)) {
            if (!lowerBound || row.pressure > lowerBound.pressure || 
                (row.pressure === lowerBound.pressure && new Date(row.test_date) > new Date(lowerBound.test_date))) {
                lowerBound = row;
            }
        }
    }

    // Find upper bound
    for (i = 0; i < pvtData.length; i++) {
        row = pvtData[i];
        if (row.pressure > pressure && 
            row.id_completion === completion && 
            new Date(row.test_date) <= new Date(lastDay)) {
            if (!upperBound || row.pressure < upperBound.pressure || 
                (row.pressure === upperBound.pressure && new Date(row.test_date) > new Date(upperBound.test_date))) {
                upperBound = row;
            }
        }
    }

    // Interpolation function
    function calculateInterpolatedValue(x1, x2, y1, y2, x) {
        if (x1 !== null && x2 !== null && y1 !== null && y2 !== null && x1 !== x2) {
            var stmt = snowflake.createStatement({
                sqlText: 'SELECT RMDE_SAM_ACC.Interpolate(:1, :2, :3, :4, :5)',
                binds: [x1, x2, y1, y2, x]
            });
            var result = stmt.execute();
            if (result.next()) {
                return result.getColumnValue(1);
            }
        }
        return null;
    }

    // Extrapolation function
    function calculateExtrapolatedValue(x1, x2, y1, y2, x) {
        if (x1 !== null && x2 !== null && y1 !== null && y2 !== null && x1 !== x2) {
            var stmt = snowflake.createStatement({
                sqlText: 'SELECT RMDE_SAM_ACC.Extrapolate(:1, :2, :3, :4, :5)',
                binds: [x1, x2, y1, y2, x]
            });
            var result = stmt.execute();
            if (result.next()) {
                return result.getColumnValue(1);
            }
        }
        return null;
    }

    // Handle exact match
    if (exactMatch) {
        interpolatedValues.push(exactMatch);
    }
    // Handle interpolation
    else if (lowerBound && upperBound) {
        row = {
            pressure: pressure,
            oil_formation_volume_factor: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.oil_formation_volume_factor, upperBound.oil_formation_volume_factor,
                pressure
            ),
            gas_formation_volume_factor: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.gas_formation_volume_factor, upperBound.gas_formation_volume_factor,
                pressure
            ),
            water_formation_volume_factor: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.water_formation_volume_factor, upperBound.water_formation_volume_factor,
                pressure
            ),
            solution_gas_oil_ratio: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.solution_gas_oil_ratio, upperBound.solution_gas_oil_ratio,
                pressure
            ),
            volatized_oil_gas_ratio: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.volatized_oil_gas_ratio, upperBound.volatized_oil_gas_ratio,
                pressure
            ),
            viscosity_oil: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.viscosity_oil, upperBound.viscosity_oil,
                pressure
            ),
            viscosity_water: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.viscosity_water, upperBound.viscosity_water,
                pressure
            ),
            viscosity_gas: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.viscosity_gas, upperBound.viscosity_gas,
                pressure
            ),
            injected_gas_formation_volume_factor: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.injected_gas_formation_volume_factor, upperBound.injected_gas_formation_volume_factor,
                pressure
            ),
            injected_water_formation_volume_factor: calculateInterpolatedValue(
                lowerBound.pressure, upperBound.pressure,
                lowerBound.injected_water_formation_volume_factor, upperBound.injected_water_formation_volume_factor,
                pressure
            )
        };
        interpolatedValues.push(row);
    }
    // Handle extrapolation
    else if (upperBound && !lowerBound) {
        matchingRows = [];
        for (i = 0; i < pvtData.length; i++) {
            row = pvtData[i];
            if (row.pressure > upperBound.pressure && 
                row.id_completion === completion && 
                new Date(row.test_date) <= new Date(lastDay)) {
                matchingRows.push(row);
            }
        }
        if (matchingRows.length > 0) {
            minPressure = matchingRows[0].pressure;
            for (i = 1; i < matchingRows.length; i++) {
                if (matchingRows[i].pressure < minPressure) {
                    minPressure = matchingRows[i].pressure;
                }
            }
            for (i = 0; i < matchingRows.length; i++) {
                if (matchingRows[i].pressure === minPressure) {
                    if (!secondBound || new Date(matchingRows[i].test_date) > new Date(secondBound.test_date)) {
                        secondBound = matchingRows[i];
                    }
                }
            }
        }
        if (secondBound) {
            row = {
                pressure: pressure,
                oil_formation_volume_factor: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.oil_formation_volume_factor, secondBound.oil_formation_volume_factor,
                    pressure
                ),
                gas_formation_volume_factor: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.gas_formation_volume_factor, secondBound.gas_formation_volume_factor,
                    pressure
                ),
                water_formation_volume_factor: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.water_formation_volume_factor, secondBound.water_formation_volume_factor,
                    pressure
                ),
                solution_gas_oil_ratio: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.solution_gas_oil_ratio, secondBound.solution_gas_oil_ratio,
                    pressure
                ),
                volatized_oil_gas_ratio: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.volatized_oil_gas_ratio, secondBound.volatized_oil_gas_ratio,
                    pressure
                ),
                viscosity_oil: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.viscosity_oil, secondBound.viscosity_oil,
                    pressure
                ),
                viscosity_water: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.viscosity_water, secondBound.viscosity_water,
                    pressure
                ),
                viscosity_gas: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.viscosity_gas, secondBound.viscosity_gas,
                    pressure
                ),
                injected_gas_formation_volume_factor: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.injected_gas_formation_volume_factor, secondBound.injected_gas_formation_volume_factor,
                    pressure
                ),
                injected_water_formation_volume_factor: calculateExtrapolatedValue(
                    upperBound.pressure, secondBound.pressure,
                    upperBound.injected_water_formation_volume_factor, secondBound.injected_water_formation_volume_factor,
                    pressure
                )
            };
            interpolatedValues.push(row);
        }
    }

    // If no values found, return nulls
    if (interpolatedValues.length === 0) {
        interpolatedValues.push({
            pressure: pressure,
            oil_formation_volume_factor: null,
            gas_formation_volume_factor: null,
            water_formation_volume_factor: null,
            solution_gas_oil_ratio: null,
            volatized_oil_gas_ratio: null,
            viscosity_oil: null,
            viscosity_water: null,
            viscosity_gas: null,
            injected_gas_formation_volume_factor: null,
            injected_water_formation_volume_factor: null
        });
    }

    // Round values and return result
    result = [];
    for (i = 0; i < interpolatedValues.length; i++) {
        row = interpolatedValues[i];
        result.push({
            PRESSURE: row.pressure !== null ? Math.round(row.pressure * 100000) / 100000 : null,
            OIL_FORMATION_VOLUME_FACTOR: row.oil_formation_volume_factor !== null ? Math.round(row.oil_formation_volume_factor * 100000) / 100000 : null,
            GAS_FORMATION_VOLUME_FACTOR: row.gas_formation_volume_factor !== null ? Math.round(row.gas_formation_volume_factor * 100000) / 100000 : null,
            WATER_FORMATION_VOLUME_FACTOR: row.water_formation_volume_factor !== null ? Math.round(row.water_formation_volume_factor * 100000) / 100000 : null,
            SOLUTION_GAS_OIL_RATIO: row.solution_gas_oil_ratio !== null ? Math.round(row.solution_gas_oil_ratio * 100000) / 100000 : null,
            VOLATIZED_OIL_GAS_RATIO: row.volatized_oil_gas_ratio !== null ? Math.round(row.volatized_oil_gas_ratio * 100000) / 100000 : null,
            VISCOSITY_OIL: row.viscosity_oil !== null ? Math.round(row.viscosity_oil * 100000) / 100000 : null,
            VISCOSITY_WATER: row.viscosity_water !== null ? Math.round(row.viscosity_water * 100000) / 100000 : null,
            VISCOSITY_GAS: row.viscosity_gas !== null ? Math.round(row.viscosity_gas * 100000) / 100000 : null,
            INJECTED_GAS_FORMATION_VOLUME_FACTOR: row.injected_gas_formation_volume_factor !== null ? Math.round(row.injected_gas_formation_volume_factor * 100000) / 100000 : null,
            INJECTED_WATER_FORMATION_VOLUME_FACTOR: row.injected_water_formation_volume_factor !== null ? Math.round(row.injected_water_formation_volume_factor * 100000) / 100000 : null
        });
    }

    return result;
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
        WHEN x < x1 OR x > x2 THEN NULL
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;
