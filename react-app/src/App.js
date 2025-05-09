import React from 'react';
import logo from './logo.svg';
import axios from 'axios';
import './App.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Button } from 'react-bootstrap';
import ClipLoader from "react-spinners/ClipLoader";
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
      query: "",
      textResults: [],
      imageResults: [],
      isLoading: false,
      noAnswer: false,
      gotResult: false,
      searchType: "TEXT",
      firstTimeSearch: true,
      searchTime: 0
    };
  }


  setSearchQuery = (query) => {
    console.log(query);
    this.setState({
      query: query
    });
    //this.getStemSearchResults(query)
  }

  convertBufferToImage = (bufferList) => {
    const imageList = [];
    for (let i = 0; i < bufferList.length; i++) {
      // const base64String = window.btoa(String.fromCharCode(...new Uint8Array(bufferList[i])));
      // console.log(bufferList[i]);
      // console.log("base64: ", base64String);

      var binary = '';
      var bytes = [].slice.call(new Uint8Array(bufferList[i]));
      bytes.forEach((b) => binary += String.fromCharCode(b));
      const base64String = window.btoa(binary);
      console.log(bufferList[i]);
      console.log(bytes);
      console.log(binary);
      console.log("base64: ", base64String);

      imageList.push(base64String);
    }
    return imageList;
  }

  getStemSearchResults = () => {
    console.log(this.state.query)
    const query = this.state.query.trim();
    if (this.state.query.length === 0) {
      return;
    }

    this.setState({
      isLoading: true,
      noAnswer: false,
      gotResult: false,
      firstTimeSearch: false,
      textResults: [],
      imageResults: [],
      optionName: "hadoop"
    })

    axios.get('http://localhost:3001/query-stem', {
      params: {
        optionName: this.state.optionName,
        searchType: this.state.searchType,
        query: query
      }
    }).then((response) => {
      console.log(response.data);

      this.setState({
        isLoading: false,
        textResults: response.data.textResult,
        imageResults: response.data.imageResult
      })

      if (response.data.success === true && response.data.result.length === 0) {
        this.setState({
          noAnswer: true,
          firstTimeSearch: true
        });
      } else {
        this.setState({
          gotResult: true,
          noAnswer: false,
          searchTime: response.data.searchTime
        })
      }
    }).catch(err => {
      this.setState({
        gotResult: false,
        noAnswer: false,
        isLoading: false
      })
      console.log(err);
    });
  }

  setSearchType = (event) => {
    console.log(event.target.value);
    this.setState({
      searchType: event.target.value
    });
  }

  setOptions = (option) => {
    console.log(option);
    this.setState({
      optionName: option
    })
  }

  componentDidMount() {
    // this.getPostCountVsLocationSortedData();
    // this.getTrendsData();
  }

  render() {
    return (
      <div>
        {/* <Navbar fluid className='sticky-top' bg="dark" variant="dark">
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
        </Navbar> */}

        <Container className="p-3">
          <Row>
            <Col lg="2">
              <div>
                <select id="options" onChange = {event => this.setOptions(event.target.value)}>
                  <option value="hadoop">Hadoop</option>
                  <option value="lucene">Lucene</option>
                </select>
              </div>
            </Col>
            <Col lg="7">
              <div className="search">
                <div className="form-group">
                  {/* <label htmlFor="formGroupExampleInput">Search</label> */}
                  <input
                    type="text"
                    className="form-control"
                    id="formGroupExampleInput"
                    placeholder='Search'
                    onChange={event => this.setSearchQuery(event.target.value)}
                  />
                </div>
              </div>
            </Col>
            <Col>
              <Button variant="outline-secondary" onClick={this.getStemSearchResults}>Search</Button>
            </Col>
          </Row>
          <Row className='mt-3'>
            <Col lg="2">
            </Col>
            <Col className="searchResultCol mb-3" lg="7">
              <div onChange={this.setSearchType.bind(this)}>
                <Row>
                  <Col lg="2">
                    <input type="radio" value="TEXT" defaultChecked name="searchtype" /> <span className='mr-3' style={{ marginLeft: "20px !important" }}>Text</span>
                  </Col>
                  <Col lg="2">
                    <input type="radio" value="IMAGE" name="searchtype" className='ml-3' /> <span className='ml-3'>Image</span>
                  </Col>
                  <Col></Col>
                </Row>
              </div>
              <ClipLoader loading={this.state.isLoading} size={15} />
              {this.state.noAnswer && !this.state.isLoading &&
                (<div style={{ fontSize: "14px" }}>
                  No search results found.
                </div>)
              }
              {/* Show text results */}
              {this.state.gotResult && (
                <div className='mt-3' style={{ fontSize: "10px" }}>
                  Search results found in <span>{this.state.searchTime}</span> seconds.
                </div>
              )}
              {this.state.searchType === "TEXT" && this.state.textResults.map(result => (
                <div className='mt-1'>
                  <a href={result.url} target="_blank">{result.docId}</a>
                  <div style={{ fontSize: "10px" }}>{result.chunkedBody}..</div>
                </div>
              ))}
              {/* Show image results */}
              {this.state.searchType === "IMAGE" ? this.state.imageResults.map(base64String =>
                (<img className='mr-3 mt-3 ml-3' src={`data:image/png;base64,${base64String}`} alt="" width="150" height="100" />)
              ) : (
                <div></div>
              )}
            </Col>
          </Row>

        </Container>
      </div>
    );
  }
}

export default App;
