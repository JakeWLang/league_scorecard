const sheetId = '1Sk1PrhvWS9oj2FSHgQnlfu-zd74C_EknYfA2cIo9ANU'
const sheetName = encodeURIComponent('Form Responses 1')
const sheetURL = `https://docs.google.com/spreadsheets/d/${sheetId}/gviz/tq?tqx=out:csv&sheet=${sheetName}`
const dateColName = 'Timestamp'
const colNameMap = {
    'Link to discord message that includes picture': 'img_link',
    'Please mark every point you scored.': 'point_cats',
    'Timestamp': 'time',
    'Total point worth': 'total_points',
    'What is your username as it appears in the discord?': 'username',
    'What unit(s) have you completed?': 'units_completed'
}


fetch(sheetURL)
    .then((response) => response.text())
    .then((fileText) => handleResponse(fileText));


function csvSplit(row) {
    return row.split(';') //.map((val) => val.substring(1, val.length - 1));
}


function parseCSV(csv) {
    const csvRows = csv.split('\n');
    const colNames = csvSplit(csvRows[0]);
    let objects = [];
    for (let i = 1; i < csvRows.length; i++) {
        let thisObject = {};
        let row = csvRows[i].split(';');

        for (let j = 0; j < row.length; j++) {
            var val = row[j]
            let colName = colNames[j]
            let fmtColName = colNameMap[colName]
            if (colName === dateColName) {
                val = new Date(val)
            }
            if (fmtColName !== "undefined") {
                colName = fmtColName
            }
            thisObject[colName] = val
        }
        objects.push(thisObject);
    }
    return objects;
}


function handleResponse(fileText) {
    fileText = fileText.replaceAll('","', ';').replaceAll('"', '')
    let sheetObjects = parseCSV(fileText);

    let totalPoints = gatherTotalPoints(sheetObjects)
    let top3 = gatherTopN(totalPoints, 3)
    let podiumSorted = podiumSort(top3)
    makePodium(podiumSorted[0], podiumSorted[1], podiumSorted[2])

}

function gatherTotalPoints(csv) {
    let totalPoints = {}

    for (let i = 0; i < csv.length; i++) {
        let row = csv[i]
        let rowUser = row['username'], rowPoints = row['total_points'];
        rowPoints = rowPoints.match('[0-9]*')[0]
        rowPoints = Number(rowPoints)


        let currUsers = Object.keys(totalPoints)
        let currUserNew = !currUsers.includes(row['username'])
        if (currUserNew) {
            totalPoints[rowUser] = rowPoints
        } else {
            totalPoints[rowUser] += rowPoints
        }
    }
    return totalPoints
}

function gatherTopN(data, n) {
    users = Object.keys(data), vals = Object.values(data)
    vals.sort(function(a, b) {
        return b - a;
    });
    vals = vals.slice(0, n)
    filtData = {}
    for (let i = 0; i < users.length; i++) {
        let activeUser = users[i]
        for (let j = 0; j < vals.length; j++) {
            let activeVal = vals[j]
            if (data[activeUser] == activeVal) {
                filtData[activeUser] = {
                    'value': activeVal,
                    'position': j + 1
                }
            }
        }
    }
    return filtData
}


function podiumSort(data) {
    // Find the user with 2, then 1, then 3
    let sortOrder = [2, 1, 3]
    let sortedUsers = [], sortedValues = []
    let users = Object.keys(data)
    for (let i = 0; i < sortOrder.length; i++) {
        let activeOrder = sortOrder[i]
        for (let j = 0; j < users.length; j++) {
            let activeUser = users[j]
            if (data[activeUser]['position'] == activeOrder) {
                sortedUsers.push(activeUser)
                sortedValues.push(data[activeUser]['value'])
            }
        }
    }
    return [sortOrder, sortedValues, sortedUsers]
}


function gatherValuesInKey(obj, key) {
    let keys = Object.keys(obj)
    let returnValues = []
    for (let i = 0; i < keys.length; i++) {
        returnValues.push(obj[keys[i]][key])
    }
    return returnValues
}


function makePodium(xVal, yVal, names) {
    // let xVal = [2, 1, 3]
    // let yVal = gatherValuesInKey(totalPoints, 'value') //Object.values(totalPoints)
    // let names = gatherValuesInKey(totalPoints, 'user')
    positionMap = {
        1: '🥇',
        2: '🥈',
        3: '🥉'
    }
    var trace1 = {
        x: names,
        y: yVal,
        type: 'bar',
        text: yVal.map(String),
        marker: {
            color: [
                'rgb(114, 153, 179)',
                'rgb(102, 201, 99)',
                'rgb(182, 130, 191)'
            ]
        }
    }

    var trace2 = {
        x: names,
        y: [yVal[0] + 10, yVal[1] + 10, yVal[2] + 10],
        mode: 'markers+text',
        type: 'scatter',
        text: xVal.map(function(val){return positionMap[val]}),
        textfont : {
            family:'Times New Roman',
            size: 30
          },
        textposition: 'above',
        marker: {
            size: 0,
            color: 'white'
         }
    }


    var data = [trace1, trace2]

    var layout = {
        yaxis: {
            visible: false
        },
        xaxis: {type: 'category'},
        showlegend: false
    }
    var config = {
        'displayModeBar': false
    }

    Plotly.newPlot('chartDiv', data, layout, config);
}
  
  