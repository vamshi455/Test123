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
// Compute LAST_DAY of vrr_date once
var lastDayStmt = snowflake.createStatement({
    sqlText: `SELECT LAST_DAY(?)`,
    binds: [VRR_DATE]
});
var lastDayResult = lastDayStmt.execute();
lastDayResult.next();
var lastDay = lastDayResult.getColumnValue(1);

// Query to get PVT data
var pvtQuery = `
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
    WHERE ID_COMPLETION = ?
      AND TEST_DATE <= ?
    ORDER BY TEST_DATE DESC
`;

var pvtStmt = snowflake.createStatement({
    sqlText: pvtQuery,
    binds: [COMPLETION, lastDay]
});

var pvtResults = pvtStmt.execute();
var pvtData = [];

while (pvtResults.next()) {
    pvtData.push({
        id_completion: pvtResults.getColumnValue("ID_COMPLETION"),
        test_date: pvtResults.getColumnValue("TEST_DATE"),
        pressure: pvtResults.getColumnValue("PRESSURE"),
        oil_formation_volume_factor: pvtResults.getColumnValue("OIL_FORMATION_VOLUME_FACTOR"),
        gas_formation_volume_factor: pvtResults.getColumnValue("GAS_FORMATION_VOLUME_FACTOR"),
        water_formation_volume_factor: pvtResults.getColumnValue("WATER_FORMATION_VOLUME_FACTOR"),
        solution_gas_oil_ratio: pvtResults.getColumnValue("SOLUTION_GAS_OIL_RATIO"),
        volatized_oil_gas_ratio: pvtResults.getColumnValue("VOLATIZED_OIL_GAS_RATIO"),
        viscosity_oil: pvtResults.getColumnValue("VISCOSITY_OIL"),
        viscosity_water: pvtResults.getColumnValue("VISCOSITY_WATER"),
        viscosity_gas: pvtResults.getColumnValue("VISCOSITY_GAS"),
        injected_gas_formation_volume_factor: pvtResults.getColumnValue("INJECTED_GAS_FORMATION_VOLUME_FACTOR"),
        injected_water_formation_volume_factor: pvtResults.getColumnValue("INJECTED_WATER_FORMATION_VOLUME_FACTOR")
    });
}

// Variables for matches and bounds
var exactMatch = null;
var lowerbound = null;
var upperbound = null;
var secondBound = null;
var interpolatedValues = [];

// Find exact match
var exactMatches = pvtData.filter(function(row) {
    return row.pressure === PRESSURE && 
           row.id_completion === COMPLETION && 
           new Date(row.test_date) <= new Date(lastDay);
});

if (exactMatches.length > 0) {
    exactMatches.sort(function(a, b) {
        return new Date(b.test_date) - new Date(a.test_date);
    });
    exactMatch = exactMatches[0];
}

// Find lowerbound
var lowerValues = pvtData.filter(function(row) {
    return row.pressure < PRESSURE && 
           row.id_completion === COMPLETION && 
           new Date(row.test_date) <= new Date(lastDay);
});

if (lowerValues.length > 0) {
    lowerValues.sort(function(a, b) {
        if (b.pressure !== a.pressure) {
            return b.pressure - a.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    lowerbound = lowerValues[0];
}

// Find upperbound
var upperValues = pvtData.filter(function(row) {
    return row.pressure > PRESSURE && 
           row.id_completion === COMPLETION && 
           new Date(row.test_date) <= new Date(lastDay);
});

if (upperValues.length > 0) {
    upperValues.sort(function(a, b) {
        if (a.pressure !== b.pressure) {
            return a.pressure - b.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    upperbound = upperValues[0];
}

// Interpolation function
var calculateInterpolatedValue = function(x1, x2, y1, y2, x) {
    if (x1 !== null && x2 !== null && y1 !== null && y2 !== null && x1 !== x2) {
        var stmt = snowflake.createStatement({
            sqlText: `SELECT RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?)`,
            binds: [x1, x2, y1, y2, x]
        });
        var result = stmt.execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }
    return null;
};

// Extrapolation function
var calculateExtrapolatedValue = function(x1, x2, y1, y2, x) {
    if (x1 !== null && x2 !== null && y1 !== null && y2 !== null && x1 !== x2) {
        var stmt = snowflake.createStatement({
            sqlText: `SELECT RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?)`,
            binds: [x1, x2, y1, y2, x]
        });
        var result = stmt.execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }
    return null;
};

// Handle exact match
if (exactMatch) {
    interpolatedValues.push(exactMatch);
}
// Handle interpolation between lowerbound and upperbound
else if (lowerbound && upperbound) {
    interpolatedValues.push({
        pressure: PRESSURE,
        oil_formation_volume_factor: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.oil_formation_volume_factor,
            upperbound.oil_formation_volume_factor,
            PRESSURE
        ),
        gas_formation_volume_factor: calculateInterpolatedValue(
            upperbound.pressure,
            lowerbound.pressure,
            upperbound.gas_formation_volume_factor,
            lowerbound.gas_formation_volume_factor,
            PRESSURE
        ),
        water_formation_volume_factor: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.water_formation_volume_factor,
            upperbound.water_formation_volume_factor,
            PRESSURE
        ),
        solution_gas_oil_ratio: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.solution_gas_oil_ratio,
            upperbound.solution_gas_oil_ratio,
            PRESSURE
        ),
        volatized_oil_gas_ratio: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.volatized_oil_gas_ratio,
            upperbound.volatized_oil_gas_ratio,
            PRESSURE
        ),
        viscosity_oil: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.viscosity_oil,
            upperbound.viscosity_oil,
            PRESSURE
        ),
        viscosity_water: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.viscosity_water,
            upperbound.viscosity_water,
            PRESSURE
        ),
        viscosity_gas: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.viscosity_gas,
            upperbound.viscosity_gas,
            PRESSURE
        ),
        injected_gas_formation_volume_factor: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.injected_gas_formation_volume_factor,
            upperbound.injected_gas_formation_volume_factor,
            PRESSURE
        ),
        injected_water_formation_volume_factor: calculateInterpolatedValue(
            lowerbound.pressure,
            upperbound.pressure,
            lowerbound.injected_water_formation_volume_factor,
            upperbound.injected_water_formation_volume_factor,
            PRESSURE
        )
    });
}
// Handle extrapolation when only upperbound exists
else if (upperbound && !lowerbound) {
    var potentialSecondBounds = pvtData.filter(function(row) {
        return row.pressure > upperbound.pressure && 
               row.id_completion === COMPLETION && 
               new Date(row.test_date) <= new Date(lastDay);
    });
    
    if (potentialSecondBounds.length > 0) {
        var minPressure = Math.min.apply(null, potentialSecondBounds.map(function(row) { return row.pressure; }));
        var matchingRows = potentialSecondBounds.filter(function(row) { return row.pressure === minPressure; });
        matchingRows.sort(function(a, b) {
            return new Date(b.test_date) - new Date(a.test_date);
        });
        secondBound = matchingRows[0];
    }
    
    if (secondBound) {
        interpolatedValues.push({
            pressure: PRESSURE,
            oil_formation_volume_factor: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.oil_formation_volume_factor,
                secondBound.oil_formation_volume_factor,
                PRESSURE
            ),
            gas_formation_volume_factor: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.gas_formation_volume_factor,
                secondBound.gas_formation_volume_factor,
                PRESSURE
            ),
            water_formation_volume_factor: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.water_formation_volume_factor,
                secondBound.water_formation_volume_factor,
                PRESSURE
            ),
            solution_gas_oil_ratio: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.solution_gas_oil_ratio,
                secondBound.solution_gas_oil_ratio,
                PRESSURE
            ),
            volatized_oil_gas_ratio: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.volatized_oil_gas_ratio,
                secondBound.volatized_oil_gas_ratio,
                PRESSURE
            ),
            viscosity_oil: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.viscosity_oil,
                secondBound.viscosity_oil,
                PRESSURE
            ),
            viscosity_water: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.viscosity_water,
                secondBound.viscosity_water,
                PRESSURE
            ),
            viscosity_gas: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.viscosity_gas,
                secondBound.viscosity_gas,
                PRESSURE
            ),
            injected_gas_formation_volume_factor: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.injected_gas_formation_volume_factor,
                secondBound.injected_gas_formation_volume_factor,
                PRESSURE
            ),
            injected_water_formation_volume_factor: calculateExtrapolatedValue(
                upperbound.pressure,
                secondBound.pressure,
                upperbound.injected_water_formation_volume_factor,
                secondBound.injected_water_formation_volume_factor,
                PRESSURE
            )
        });
    }
}

// If no values found, return nulls
if (interpolatedValues.length === 0) {
    interpolatedValues.push({
        pressure: PRESSURE,
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
var result = interpolatedValues.map(function(row) {
    return {
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
    };
});

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
