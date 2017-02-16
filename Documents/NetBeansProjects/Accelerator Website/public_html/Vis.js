/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
var lanes = ['Microsoft','Barclays'],
     lanesLength = lanes.length,
     timeBegin = '2016-01-01',
     timeEnd = '2017-01-01';
     
var data = [
  {
    "lane": 0,
    "batchName": "Jan2016",
    "startDate": "2016-01-01",
    "endDate": "2016-07-01",
    "startups": [
      {
        "startupName": "BitLove",
        "description": "Randomize your next date and pay using bitcoin"
      },
      {
        "startupName": "Prosper.it",
        "description": "Create a tailor made financial education with our online videos, excersises and tutorials"
      },
      {
        "startupName": "Pectoral",
        "description": "Match a gym buddy in your area"
      }
    ]
  },
  {
    "lane": 0,
    "batchName": "July2016",
    "startDate": "2016-07-31",
    "endDate": "2016-12-31",
    "startups": [
      {
        "startupName": "Ai-inspired",
        "description": "Executive training programs on the risks of AI"
      },
      {
        "startupName": "Pre-game",
        "description": "Connect to Spotify/Apple music profiles and play music that everyone likes"
      }
    ]

  },
  {
    "lane": 1,
    "batchName": "July2016",
    "startDate": "2016-07-31",
    "endDate": "2016-12-31",
    "startups": [
      {
        "startupName": "Friendly.Ai",
        "description": "A chatbot that helps kids perfect sentence structure and develop better vocabularly"
      },
      {
        "startupName": "Homely",
        "description": "Creating a microwave which you can control with a mobile device"
      }
    ]

  }
]

var d = new Date(timeBegin);
console.log(d);



//Create the margins of the plot
var m = [20, 15, 15, 120], //top right bottom left
w = 960 - m[1] - m[3],
h = 500 - m[0] - m[2],
miniHeight = lanesLength * 12 + 50,
mainHeight = h - miniHeight - 50;

//
var x = d3.scaleLinear().domain([]);

 

