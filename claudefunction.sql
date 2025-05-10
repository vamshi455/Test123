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
// Query to get PVT data with end dates
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
        INJECTED_WATER_FORMATION_VOLUME_FACTOR,
        COALESCE(
            (SELECT min(TEST_DATE)
             FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS cpvt
             WHERE cpvt.TEST_DATE > RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS.TEST_DATE
               AND cpvt.ID_COMPLETION = RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS.ID_COMPLETION),
            '9999-12-31'
        ) AS END_DATE
    FROM RMDE_SAM_ACC.COMPLETION_PVT_CHARACTERISTICS
    WHERE ID_COMPLETION = ?
      AND TEST_DATE <= LAST_DAY(?)
      AND TEST_DATE >= ?
    ORDER BY TEST_DATE DESC
`;

var pvtStmt = snowflake.createStatement({
    sqlText: pvtQuery,
    binds: [COMPLETION, VRR_DATE, PRESSURE]
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
        injected_water_formation_volume_factor: pvtResults.getColumnValue("INJECTED_WATER_FORMATION_VOLUME_FACTOR"),
        end_date: pvtResults.getColumnValue("END_DATE")
    });
}

// Table variables equivalent
var exactMatch = [];
var lowerbound = [];
var upperbound = [];
var secondBound = [];
var interpolatedValues = [];

// Get last day of month once
var lastDayStmt = snowflake.createStatement({
    sqlText: "SELECT LAST_DAY(?)",
    binds: [VRR_DATE]
});
var lastDayResult = lastDayStmt.execute();
lastDayResult.next();
var lastDayOfMonth = lastDayResult.getColumnValue(1);

// Find exact match
var exactMatches = [];
for (var i = 0; i < pvtData.length; i++) {
    var row = pvtData[i];
    if (row.pressure === PRESSURE && 
        row.id_completion === COMPLETION && 
        new Date(row.test_date) <= new Date(lastDayOfMonth)) {
        exactMatches.push(row);
    }
}

if (exactMatches.length > 0) {
    // Sort by test_date desc
    exactMatches.sort(function(a, b) {
        return new Date(b.test_date) - new Date(a.test_date);
    });
    exactMatch.push(exactMatches[0]);
}

// Find lowerbound
var lowerValues = [];
for (var i = 0; i < pvtData.length; i++) {
    var row = pvtData[i];
    if (row.pressure < PRESSURE && 
        row.id_completion === COMPLETION && 
        new Date(row.test_date) <= new Date(lastDayOfMonth)) {
        lowerValues.push(row);
    }
}

if (lowerValues.length > 0) {
    // Sort by pressure desc, then test_date desc
    lowerValues.sort(function(a, b) {
        if (b.pressure !== a.pressure) {
            return b.pressure - a.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    lowerbound.push(lowerValues[0]);
}

// Find upperbound
var upperValues = [];
for (var i = 0; i < pvtData.length; i++) {
    var row = pvtData[i];
    if (row.pressure > PRESSURE && 
        row.id_completion === COMPLETION && 
        new Date(row.test_date) <= new Date(lastDayOfMonth)) {
        upperValues.push(row);
    }
}

if (upperValues.length > 0) {
    // Sort by pressure asc, then test_date desc
    upperValues.sort(function(a, b) {
        if (a.pressure !== b.pressure) {
            return a.pressure - b.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    upperbound.push(upperValues[0]);
}

// Function to call the Interpolate database function
function calculateInterpolatedValue(x1, x2, y1, y2, x) {
    // Use Snowflake's built-in interpolate function if both values exist
    if (x1 !== null && x2 !== null && y1 !== null && y2 !== null) {
        var stmt = snowflake.createStatement({
            sqlText: "SELECT RMDE_SAM_ACC.Interpolate(?, ?, ?, ?, ?)",
            binds: [x1, x2, y1, y2, x]
        });
        var result = stmt.execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }
    return null;
}

// Function to call the Extrapolate database function
function calculateExtrapolatedValue(x1, x2, y1, y2, x) {
    // Use Snowflake's built-in extrapolate function if both values exist
    if (x1 !== null && x2 !== null && y1 !== null && y2 !== null) {
        var stmt = snowflake.createStatement({
            sqlText: "SELECT RMDE_SAM_ACC.Extrapolate(?, ?, ?, ?, ?)",
            binds: [x1, x2, y1, y2, x]
        });
        var result = stmt.execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }
    return null;
}

// Interpolate with lowerbound and upperbound if available
if (lowerbound.length === 1 && upperbound.length === 1 && interpolatedValues.length === 0) {
    interpolatedValues.push({
        pressure: PRESSURE,
        oil_formation_volume_factor: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].oil_formation_volume_factor,
            upperbound[0].oil_formation_volume_factor,
            PRESSURE
        ),
        gas_formation_volume_factor: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].gas_formation_volume_factor,
            upperbound[0].gas_formation_volume_factor,
            PRESSURE
        ),
        water_formation_volume_factor: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].water_formation_volume_factor,
            upperbound[0].water_formation_volume_factor,
            PRESSURE
        ),
        solution_gas_oil_ratio: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].solution_gas_oil_ratio,
            upperbound[0].solution_gas_oil_ratio,
            PRESSURE
        ),
        volatized_oil_gas_ratio: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].volatized_oil_gas_ratio,
            upperbound[0].volatized_oil_gas_ratio,
            PRESSURE
        ),
        viscosity_oil: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_oil,
            upperbound[0].viscosity_oil,
            PRESSURE
        ),
        viscosity_water: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_water,
            upperbound[0].viscosity_water,
            PRESSURE
        ),
        viscosity_gas: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_gas,
            upperbound[0].viscosity_gas,
            PRESSURE
        ),
        injected_gas_formation_volume_factor: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].injected_gas_formation_volume_factor,
            upperbound[0].injected_gas_formation_volume_factor,
            PRESSURE
        ),
        injected_water_formation_volume_factor: calculateInterpolatedValue(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].injected_water_formation_volume_factor,
            upperbound[0].injected_water_formation_volume_factor,
            PRESSURE
        )
    });
}

// Handle the case where there's an upperbound but no lowerbound
var countMoreThanUpperbound = 0;
for (var i = 0; i < pvtData.length; i++) {
    var row = pvtData[i];
    if (row.id_completion === COMPLETION && 
        row.pressure > PRESSURE && 
        new Date(row.test_date) <= new Date(lastDayOfMonth)) {
        countMoreThanUpperbound++;
    }
}

if (upperbound.length === 1 && lowerbound.length === 0 && exactMatch.length === 0 && countMoreThanUpperbound > 1) {
    
    // Find secondBound
    var potentialSecondBounds = [];
    for (var i = 0; i < pvtData.length; i++) {
        var row = pvtData[i];
        if (row.pressure > upperbound[0].pressure && 
            row.id_completion === COMPLETION && 
            new Date(row.test_date) <= new Date(lastDayOfMonth)) {
            potentialSecondBounds.push(row);
        }
    }
    
    if (potentialSecondBounds.length > 0) {
        // Find the minimum pressure greater than upperbound
        var minPressure = Number.MAX_VALUE;
        for (var i = 0; i < potentialSecondBounds.length; i++) {
            if (potentialSecondBounds[i].pressure < minPressure) {
                minPressure = potentialSecondBounds[i].pressure;
            }
        }
        
        var matchingRows = [];
        for (var i = 0; i < potentialSecondBounds.length; i++) {
            if (potentialSecondBounds[i].pressure === minPressure) {
                matchingRows.push(potentialSecondBounds[i]);
            }
        }
        
        // Sort by test_date desc and take the first one
        matchingRows.sort(function(a, b) {
            return new Date(b.test_date) - new Date(a.test_date);
        });
        
        if (matchingRows.length > 0) {
            secondBound.push(matchingRows[0]);
        }
    }
    
    // If there's an exact match, use it
    if (exactMatch.length === 1) {
        interpolatedValues.push(exactMatch[0]);
    }
    // If there's a secondBound but no exact match, extrapolate
    else if (upperbound.length === 1 && secondBound.length === 1 && exactMatch.length === 0) {
        interpolatedValues.push({
            pressure: PRESSURE,
            oil_formation_volume_factor: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].oil_formation_volume_factor,
                secondBound[0].oil_formation_volume_factor,
                PRESSURE
            ),
            gas_formation_volume_factor: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].gas_formation_volume_factor,
                secondBound[0].gas_formation_volume_factor,
                PRESSURE
            ),
            water_formation_volume_factor: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].water_formation_volume_factor,
                secondBound[0].water_formation_volume_factor,
                PRESSURE
            ),
            solution_gas_oil_ratio: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].solution_gas_oil_ratio,
                secondBound[0].solution_gas_oil_ratio,
                PRESSURE
            ),
            volatized_oil_gas_ratio: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].volatized_oil_gas_ratio,
                secondBound[0].volatized_oil_gas_ratio,
                PRESSURE
            ),
            viscosity_oil: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_oil,
                secondBound[0].viscosity_oil,
                PRESSURE
            ),
            viscosity_water: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_water,
                secondBound[0].viscosity_water,
                PRESSURE
            ),
            viscosity_gas: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_gas,
                secondBound[0].viscosity_gas,
                PRESSURE
            ),
            injected_gas_formation_volume_factor: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].injected_gas_formation_volume_factor,
                secondBound[0].injected_gas_formation_volume_factor,
                PRESSURE
            ),
            injected_water_formation_volume_factor: calculateExtrapolatedValue(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].injected_water_formation_volume_factor,
                secondBound[0].injected_water_formation_volume_factor,
                PRESSURE
            )
        });
    }
}

// If no values have been found, add null values
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

// Round values and return the result
var result = [];
for (var i = 0; i < interpolatedValues.length; i++) {
    var row = interpolatedValues[i];
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

-- Make sure the Extrapolate function exists - create it if it doesn't
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
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;

-- Make sure the Interpolate function exists - create it if it doesn't
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
        ELSE y1 + (x - x1) * (y2 - y1) / (x2 - x1)
    END
$$;
