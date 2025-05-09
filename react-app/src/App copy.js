import React from 'react';
import logo from './logo.svg';
import axios from 'axios';
import './App.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Image from 'react-bootstrap/Image';
import Figure from 'react-bootstrap/Figure';
import { PieChart } from './PieChart';
import { BarChart } from './BarChart';
import clusterImg from './cluster.png';
import nonNormalizedBoxplotImg from './NonNormalized_LogReg.png';
import nonNorm from './non-norm.png';
import normalizedBoxplotImg1 from './Normalized_LogReg.png';
import norm from './norm.png';
import usageUSImg from './usage_usa.png';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import timeTitleLen from './titleLen_vs_time.png';
import timeTagSpec from './tagSpec_vs_time.png';
import numTime from './numPopTag_vs_time.png';
import bodyLenTime from './bodyLen_vs_time.png';
import avgTime from './avgPop_vs_time.png';
import tagPairs from './tag-pairs.png';
import cluster50 from './cluster20_50x50_51.png';

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      postCountVsLocationSortedDataOptions: {},
      trendsOptions: {},
      trendsOptionsYearList: [],
      trendsOptionsLanguageList: []
    };
  }

  getPostCountVsLocationSortedData() {
    axios.get('http://localhost:3001/location/post-count-location-sorted').then((response) => {
      console.log(response.data.jsonData);
      let size;
      const options = {
        title: {
          text: "Post Count Vs Location Bar Chart"
        },
        axisX: {
          interval: 1,
        },
        axisY: {
          title: "Post Count (100K)"
        }
      };

      if (response.data.jsonData.length > 20) {
        size = 20;
      } else {
        size = response.data.jsonData.length;
      }

      const data = [];
      const dataPoints = [];

      for (let i = 0; i < size; i++) {
        let y = parseFloat(response.data.jsonData[i]["count"]) / 100000;
        dataPoints.push({
          y: y,
          label: response.data.jsonData[i]["Location"]
        })
      }

      const json = {
        type: "column",
        animationEnabled: true,
        dataPoints: dataPoints
      };

      data.push(json);

      options.data = data;
      console.log(options);

      this.setState({
        postCountVsLocationSortedDataOptions: options
      })
    }).catch(err => {
      console.log(err);
    });
  }


  setTrendOptions(dataBig) {
    const jsonDataYearList = dataBig.jsonDataYearList;
    const jsonDataLanguageList = dataBig.jsonDataLanguageList;
    const trendsOptionsLanguageList = [];
    const trendsOptionsYearList = [];

    // pie chart
    jsonDataYearList.forEach(element => {
      let size;
      const options = {
        title: {
          text: "Language trends of " + element.year
        },
        axisY: {
          title: "User Count (10K)"
        }
      };

      if (element.jsonList.length > 10) {
        size = 10;
      } else {
        size = element.jsonList.length;
      }

      const data = [];
      const dataPoints = [];

      for (let i = 0; i < size; i++) {
        let y = parseFloat(element.jsonList[i]["count"]) / 10000;
        dataPoints.push({
          label: element.jsonList[i]["tagName"],
          y: y
        })
      }

      const json = {
        type: "pie",
        startAngle: 75,
        toolTipContent: "<b>{label}</b>: {y}%",
        showInLegend: "true",
        legendText: "{label}",
        indexLabelFontSize: 16,
        indexLabel: "{label} - {y}%",
        dataPoints: dataPoints
      };

      data.push(json);

      options.data = data;
      console.log(options);
      trendsOptionsYearList.push(options);
    });

    // column chart
    jsonDataLanguageList.forEach(element => {
      let size;
      const options = {
        title: {
          text: "Trends of " + element.language
        },
        axisX: {
          interval: 1
        },
        axisY: {
          title: "User Count (10K)"
        }
      };

      if (element.jsonList.length > 10) {
        size = 10;
      } else {
        size = element.jsonList.length;
      }

      const data = [];
      const dataPoints = [];

      for (let i = 0; i < size; i++) {
        let y = parseFloat(element.jsonList[i]["count"]) / 10000;
        dataPoints.push({
          label: element.jsonList[i][element.language],
          y: y
        })
      }

      const json = {
        type: "column",
        dataPoints: dataPoints
      };

      data.push(json);

      options.data = data;
      console.log(options);
      trendsOptionsLanguageList.push(options);
    });

    console.log("Year");
    console.log(trendsOptionsYearList);
    console.log("Language");
    console.log(trendsOptionsLanguageList);

    this.setState({
      trendsOptionsYearList: trendsOptionsYearList,
      trendsOptionsLanguageList: trendsOptionsLanguageList
    });
  }

  getTrendsData(fileName) {
    axios.get('http://localhost:3001/trends/particular-year-trend').then((response) => {
      console.log(response.data);
      this.setTrendOptions(response.data);
    }).catch(err => {
      console.log(err);
    });
  }

  componentDidMount() {
    this.getPostCountVsLocationSortedData();
    this.getTrendsData();
  }

  render() {
    return (
      <div>
        <Navbar fluid className='sticky-top' bg="dark" variant="dark">
          <Container>
            <Navbar.Brand href="#home">BigData</Navbar.Brand>
            <Nav className="me-auto">
              <Nav.Link href="#languageTrends">LanguageTrends</Nav.Link>
              <Nav.Link href="#geoDistribution">GeoDistribution</Nav.Link>
              <Nav.Link href="#co_occurrenceFrequency">Co-occurranceFrequency</Nav.Link>
              <Nav.Link href="#responseTimePrediction">ResponseTime</Nav.Link>
              <Nav.Link href="#userExpertise">UserExpertise</Nav.Link>
              <Nav.Link href="#clusteredTags">ClusteredTags</Nav.Link>
            </Nav>
          </Container>
        </Navbar>
        
        <Container className="p-3">
          <Row>
            <Col>
              <h1 id="languageTrends" className='my-3 text-center'>
                <strong>Most Popular Programming Languages or Tools over Years</strong>
              </h1>
            </Col>
          </Row>
          {/* Render the language and framewrorkk usage per year */}
          {this.state.trendsOptionsYearList.map((option) => (
            <Row className='py-3'>
              <Col className='my-3 mx-auto'>
                <PieChart options={option} />
              </Col>
            </Row>

          ))}
          {/* Render the top three language usage per year */}
          {this.state.trendsOptionsLanguageList.map((option) => (
            <Row className='py-3'>
              <Col className='my-3 mx-auto'>
                <BarChart options={option} />
              </Col>
            </Row>
          ))}
          <Row>
            <Col>
              <h1 id="geoDistribution" className='my-3 text-center'>
                <strong>Geographical Distribution of StackOverflow Usage</strong>
              </h1>
            </Col>
          </Row>
          <Row className="py-3">
            <Col className="my-3 mx-auto">
              <BarChart options={this.state.postCountVsLocationSortedDataOptions} />
            </Col>
          </Row>
          <Row className='py-3'>
            <Col className='text-center my-3 mx-auto'>
              <h1>Post Count vs US States</h1>
              <Image src={usageUSImg} width={700} height={400} />
            </Col>
          </Row>
          <Row>
            <Col>
              <h1 id="co_occurrenceFrequency" className='my-3 text-center'>
                <strong>Co-occurrence Frequency of Tag-Pairs</strong>
              </h1>
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <Image src={tagPairs} width={700} height={400} />
            </Col>
          </Row>
          <Row>
            <Col>
              <h1 id="responseTimePrediction" className='my-3 text-center'>
                <strong>Response Time Prediction of a Post</strong>
              </h1>
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <h1>Scatter Plots</h1>
              <Image src={timeTagSpec} width={700} height={400} />
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <Image src={timeTitleLen} width={700} height={400} />
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <Image src={avgTime} width={700} height={400} />
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <Image src={numTime} width={700} height={400} />
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <Image src={bodyLenTime} width={700} height={400} />
            </Col>
          </Row>
          <Row>
            <Col>
              <h1 id="userExpertise" className='my-3 text-center'>
                <strong>User Expertise Level Prediction</strong>
              </h1>
            </Col>
          </Row>
          <Row id="" className='py-3'>
            <Col className='text-center'>
              <h1>Boxplots for the accuracy of 10-fold cross-validation models trained on non-normalized data</h1>
              <Image src={nonNormalizedBoxplotImg} width={700} height={400} />
            </Col>
          </Row>
          <Row className='py-3'>
            <Col className='text-center my-3 mx-auto'>
              <h1>Boxplots for the accuracy of 10-fold cross-validation models trained on normalized data</h1>
              <Image src={normalizedBoxplotImg1} width={700} height={400} />
            </Col>
          </Row>

          <Row className='py-3'>
            <Col className='text-center my-3 mx-auto'>
              <Image src={nonNorm} width={700} height={400} />
            </Col>
          </Row>
          <Row className='py-3'>
            <Col className='text-center my-3 mx-auto'>
              <Image src={norm} width={700} height={400} />
            </Col>
          </Row>

          <Row>
            <Col>
              <h1 id="clusteredTags" className='my-3 text-center'>
                <strong>Clusterification of Co-occurring Tag-Pairs</strong>
              </h1>
            </Col>
          </Row>
          <Row>
            <Col className='text-center'>
              <Image src={clusterImg} width={700} height={400} />
            </Col>
            <div>
              <p>
                <strong>C1:</strong> (python, python3),
                <strong>C2:</strong> (ruby-on-rails, ruby),
                <strong>C3:</strong> (swift, xcode, ios),
                <strong>C4:</strong> (javascript, jquery),
                <strong>C5:</strong> (html, css),
                <strong>C6:</strong> (pandas, django, numpy, matplotlib),
                <strong>C7:</strong> (objective-c),
                <strong>C8:</strong> (android, java, html5, monaca, onsen-ui, node.js, swift3),
                <strong>C9:</strong> (C#, .net, visual-studio, unity3d),
                <strong>C10:</strong> (php, mysql, laravel)
              </p>
            </div>
          </Row>
          <Row>
            <Col className='text-center'>
              <Image src={cluster50} width={700} height={400} />
            </Col>
            <div>
              <p>
                <strong>C1:</strong> (python, python3),
                <strong>C2:</strong> (ruby-on-rails, ruby),
                <strong>C3:</strong> (swift, xcode, ios),
                <strong>C4:</strong> (javascript, jquery, html, css),
                <strong>C5:</strong> (pandas),
                <strong>C6:</strong> (objective-c),
                <strong>C7:</strong> (android, android-studio),
                <strong>C8:</strong> (java, spring, spring-boot),
                <strong>C9:</strong> (django, numpy, matplotlib, tensorflow, machine-learning, anaconda),
                <strong>C10:</strong> (c\#, .net, visual-studio, unity3d, wpf),
                <strong>C11:</strong> (html5, node.js, vue.js, reactjs),
                <strong>C12:</strong> (php, mysql),
                <strong>C13:</strong> (laravel, cakephp, wordpress),
                <strong>C14:</strong> (monaca, onsen-ui),
                <strong>C15:</strong> (swift3, swift4, realm),
                <strong>C16:</strong> (linux, ubuntu, centos),
                <strong>C17:</strong> (iphone),
                <strong>C18:</strong> (sql),
                <strong>C19:</strong> (rails-activerecord),
                <strong>C20:</strong> (c++, c).
              </p>
            </div>
          </Row>
        </Container>
      </div>
    );
  }
}

export default App;
