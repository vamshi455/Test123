CREATE OR REPLACE FUNCTION vrr.InterpolatePVTCompletionTest(
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
function extrapolate(x1, x2, y1, y2, x) {
    if (x1 === x2) return y1;
    return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
}

// Get the last day of the month for vrr_date
function lastDayOfMonth(date) {
    let d = new Date(date);
    d.setMonth(d.getMonth() + 1);
    d.setDate(0);
    return d.toISOString().split('T')[0];
}

const monthEnd = lastDayOfMonth(VRR_DATE);

// Query to get PVT data with end dates
const pvtQuery = `
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
            '9999-12-31'
        ) AS END_DATE
    FROM vrr.COMPLETION_PVT_CHARACTERISTICS
    WHERE ID_COMPLETION = ?
      AND TEST_DATE <= ?
      AND TEST_DATE >= ?
    ORDER BY TEST_DATE DESC
`;

const pvtStmt = snowflake.createStatement({
    sqlText: pvtQuery,
    binds: [COMPLETION, monthEnd, PRESSURE]
});

const pvtResults = pvtStmt.execute();
const pvtData = [];

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
let exactMatch = [];
let lowerbound = [];
let upperbound = [];
let secondBound = [];
let interpolatedValues = [];

// Find exact match
const exactMatches = pvtData.filter(row => 
    row.pressure === PRESSURE && 
    row.id_completion === COMPLETION && 
    row.test_date <= monthEnd
);

if (exactMatches.length > 0) {
    // Sort by test_date desc and take the first one
    exactMatches.sort((a, b) => new Date(b.test_date) - new Date(a.test_date));
    exactMatch.push(exactMatches[0]);
}

// Find lowerbound
const lowerValues = pvtData.filter(row => 
    row.pressure < PRESSURE && 
    row.id_completion === COMPLETION && 
    row.test_date <= monthEnd
);

if (lowerValues.length > 0) {
    // Sort by pressure desc, then test_date desc
    lowerValues.sort((a, b) => {
        if (b.pressure !== a.pressure) {
            return b.pressure - a.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    lowerbound.push(lowerValues[0]);
}

// Find upperbound
const upperValues = pvtData.filter(row => 
    row.pressure > PRESSURE && 
    row.id_completion === COMPLETION && 
    row.test_date <= monthEnd
);

if (upperValues.length > 0) {
    // Sort by pressure asc, then test_date desc
    upperValues.sort((a, b) => {
        if (a.pressure !== b.pressure) {
            return a.pressure - b.pressure;
        }
        return new Date(b.test_date) - new Date(a.test_date);
    });
    upperbound.push(upperValues[0]);
}

// Interpolate with lowerbound and upperbound if available
if (lowerbound.length === 1 && upperbound.length === 1 && interpolatedValues.length === 0) {
    interpolatedValues.push({
        pressure: PRESSURE,
        oil_formation_volume_factor: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].oil_formation_volume_factor,
            upperbound[0].oil_formation_volume_factor,
            PRESSURE
        ),
        gas_formation_volume_factor: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].gas_formation_volume_factor,
            upperbound[0].gas_formation_volume_factor,
            PRESSURE
        ),
        water_formation_volume_factor: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].water_formation_volume_factor,
            upperbound[0].water_formation_volume_factor,
            PRESSURE
        ),
        solution_gas_oil_ratio: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].solution_gas_oil_ratio,
            upperbound[0].solution_gas_oil_ratio,
            PRESSURE
        ),
        volatized_oil_gas_ratio: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].volatized_oil_gas_ratio,
            upperbound[0].volatized_oil_gas_ratio,
            PRESSURE
        ),
        viscosity_oil: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_oil,
            upperbound[0].viscosity_oil,
            PRESSURE
        ),
        viscosity_water: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_water,
            upperbound[0].viscosity_water,
            PRESSURE
        ),
        viscosity_gas: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].viscosity_gas,
            upperbound[0].viscosity_gas,
            PRESSURE
        ),
        injected_gas_formation_volume_factor: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].injected_gas_formation_volume_factor,
            upperbound[0].injected_gas_formation_volume_factor,
            PRESSURE
        ),
        injected_water_formation_volume_factor: extrapolate(
            lowerbound[0].pressure,
            upperbound[0].pressure,
            lowerbound[0].injected_water_formation_volume_factor,
            upperbound[0].injected_water_formation_volume_factor,
            PRESSURE
        )
    });
}

// Handle the case where there's an upperbound but no lowerbound
if (upperbound.length === 1 && lowerbound.length === 0 && exactMatch.length === 0 && 
    pvtData.filter(row => 
        row.id_completion === COMPLETION && 
        row.pressure > PRESSURE && 
        row.test_date <= monthEnd
    ).length > 1) {
    
    // Find secondBound
    const potentialSecondBounds = pvtData.filter(row => 
        row.pressure > upperbound[0].pressure && 
        row.id_completion === COMPLETION && 
        row.test_date <= monthEnd
    );
    
    if (potentialSecondBounds.length > 0) {
        // Find the minimum pressure greater than upperbound
        const minPressure = Math.min(...potentialSecondBounds.map(row => row.pressure));
        const matchingRows = potentialSecondBounds.filter(row => row.pressure === minPressure);
        
        // Sort by test_date desc and take the first one
        matchingRows.sort((a, b) => new Date(b.test_date) - new Date(a.test_date));
        secondBound.push(matchingRows[0]);
    }
    
    // If there's an exact match, use it
    if (exactMatch.length === 1) {
        interpolatedValues.push(exactMatch[0]);
    }
    // If there's a secondBound but no exact match, extrapolate
    else if (upperbound.length === 1 && secondBound.length === 1 && exactMatch.length === 0) {
        interpolatedValues.push({
            pressure: PRESSURE,
            oil_formation_volume_factor: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].oil_formation_volume_factor,
                secondBound[0].oil_formation_volume_factor,
                PRESSURE
            ),
            gas_formation_volume_factor: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].gas_formation_volume_factor,
                secondBound[0].gas_formation_volume_factor,
                PRESSURE
            ),
            water_formation_volume_factor: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].water_formation_volume_factor,
                secondBound[0].water_formation_volume_factor,
                PRESSURE
            ),
            solution_gas_oil_ratio: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].solution_gas_oil_ratio,
                secondBound[0].solution_gas_oil_ratio,
                PRESSURE
            ),
            volatized_oil_gas_ratio: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].volatized_oil_gas_ratio,
                secondBound[0].volatized_oil_gas_ratio,
                PRESSURE
            ),
            viscosity_oil: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_oil,
                secondBound[0].viscosity_oil,
                PRESSURE
            ),
            viscosity_water: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_water,
                secondBound[0].viscosity_water,
                PRESSURE
            ),
            viscosity_gas: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].viscosity_gas,
                secondBound[0].viscosity_gas,
                PRESSURE
            ),
            injected_gas_formation_volume_factor: extrapolate(
                upperbound[0].pressure,
                secondBound[0].pressure,
                upperbound[0].injected_gas_formation_volume_factor,
                secondBound[0].injected_gas_formation_volume_factor,
                PRESSURE
            ),
            injected_water_formation_volume_factor: extrapolate(
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
const result = interpolatedValues.map(row => ({
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
}));

return result;
$$;
