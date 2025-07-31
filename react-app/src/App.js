import React from 'react';
import axios from 'axios';
import './App.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Button } from 'react-bootstrap';
import ClipLoader from "react-spinners/ClipLoader";

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
      searchTime: 0,
      hasSearched: false,
      optionName: "tfidf",
      currentPage: 1,
      resultsPerPage: 10,
      showModal: false,
      selectedImage: null,
      currentImageIndex: 0
    };

    this.resultsRef = React.createRef();
  }

  setSearchQuery = (query) => {
    this.setState({ query });
  }

  getStemSearchResults = () => {
    const query = this.state.query.trim();
    if (query.length === 0) return;

    this.setState({
      isLoading: true,
      hasSearched: true,
      noAnswer: false,
      gotResult: false,
      firstTimeSearch: false,
      textResults: [],
      imageResults: [],
      currentPage: 1
    });

    const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:3001';

    axios.get(`${API_BASE}/query-stem`, {
      params: {
        optionName: this.state.optionName,
        searchType: this.state.searchType,
        query: query
      }
    }).then((response) => {
      this.setState({
        isLoading: false,
        textResults: response.data.textResult,
        imageResults: response.data.imageResult
      });

      if (response.data.success === true && response.data.result.length === 0) {
        this.setState({ noAnswer: true, firstTimeSearch: true });
      } else {
        this.setState({
          gotResult: true,
          noAnswer: false,
          searchTime: response.data.searchTime
        });
      }
    }).catch(err => {
      this.setState({
        gotResult: false,
        noAnswer: false,
        isLoading: false
      });
      console.log(err);
    });
  }

  setSearchType = (event) => {
    this.setState({ searchType: event.target.value, currentPage: 1 });
  }

  scrollToResults = (newPage) => {
    this.setState({ currentPage: newPage }, () => {
      if (this.resultsRef.current) {
        this.resultsRef.current.scrollIntoView({ behavior: 'smooth' });
      }
    });
  }
    openModal = (filename) => {
      window.scrollTo(0, 0); 
      const index = this.state.imageResults.indexOf(filename);
      this.setState({ selectedImage: filename, showModal: true, currentImageIndex: index });
    };


  closeModal = () => {
    this.setState({ selectedImage: null, showModal: false });
  }

  prevImage = () => {
    this.setState(prev => {
      const newIndex = (prev.currentImageIndex - 1 + prev.imageResults.length) % prev.imageResults.length;
      return {
        selectedImage: prev.imageResults[newIndex],
        currentImageIndex: newIndex
      };
    });
  }

  nextImage = () => {
    this.setState(prev => {
      const newIndex = (prev.currentImageIndex + 1) % prev.imageResults.length;
      return {
        selectedImage: prev.imageResults[newIndex],
        currentImageIndex: newIndex
      };
    });
  }

  shareImage = (url) => {
    navigator.clipboard.writeText(url);
    alert("Image URL copied to clipboard!");
  }

  render() {
    const S3_BUCKET_URL = process.env.REACT_APP_S3_BUCKET_URL || 'https://your-bucket-name.s3.amazonaws.com';

    const {
      currentPage, resultsPerPage, searchType,
      textResults, imageResults, showModal, selectedImage
    } = this.state;

    const results = searchType === "TEXT" ? textResults : imageResults;
    const totalResults = results.length;
    const startIndex = (currentPage - 1) * resultsPerPage;
    const endIndex = startIndex + resultsPerPage;
    const paginatedResults = results.slice(startIndex, endIndex);

    return (
      <div>

        <Container fluid>
          <Row className="justify-content-center">
            <Col md="auto">
              <h1 className="branding-title">WikiSearch IR</h1>
              {!this.state.hasSearched && (
                <p className="branding-subtitle">
                  Real-time ranked retrieval over a curated Wikipedia corpus.
                </p>
              )}
            </Col>
          </Row>

          <div className={`search-header ${this.state.hasSearched ? 'float-up' : ''}`}>
            <Row className="mt-4 justify-content-center align-items-center">
              <Col lg={10}>
                <div className="d-flex gap-2">
                  <input
                    type="text"
                    className="form-control form-control-lg flex-grow-1"
                    placeholder="🔍 Search football or world war topics (i.e. Fifa World Cup or Dunkirk)..."
                    onChange={event => this.setSearchQuery(event.target.value)}
                  />
                  <select
                    className="form-select form-select-lg"
                    onChange={(e) => this.setState({ optionName: e.target.value })}
                  >
                    <option value="tfidf">TF-IDF</option>
                    <option value="bm25">BM25</option>
                  </select>
                  <Button className="btn-lg" variant="primary" onClick={this.getStemSearchResults}>
                    {this.state.isLoading ? <ClipLoader size={18} color="#fff" /> : "Search"}
                  </Button>
                </div>
              </Col>
            </Row>

            <Row className="mt-3 justify-content-center">
              <Col lg={9}>
                <div className="alert alert-info p-2 small shadow-sm">
                  ⚠️ This demo search engine runs on a limited test dataset due to resource constraints. Ranking result does not represent full-scale performance.
                </div>
              </Col>
            </Row>

            {this.state.hasSearched && (
              <div className="radio-toggle mt-4 mb-3 text-center">
                <label>
                  <input type="radio" value="TEXT" checked={searchType === "TEXT"} onChange={this.setSearchType} />
                  <span className="ms-2 me-4">Text</span>
                </label>
                <label>
                  <input type="radio" value="IMAGE" checked={searchType === "IMAGE"} onChange={this.setSearchType} />
                  <span className="ms-2">Image</span>
                </label>
              </div>
            )}

            {this.state.hasSearched && (
              <Row ref={this.resultsRef} className="justify-content-center results-section">
                <Col lg={8}>
                  {this.state.gotResult && (
                    <div className="mt-3" style={{ fontSize: "12px" }}>
                      Search results found in <span>{this.state.searchTime}</span> seconds.
                    </div>
                  )}

                  {this.state.noAnswer && !this.state.isLoading && (
                    <div style={{ fontSize: "14px" }}>
                      No search results found.
                    </div>
                  )}

                  {searchType === "TEXT" && paginatedResults.map((result, index) => (
                    <div key={index} className="mt-3 search-result-animated" style={{ animationDelay: `${index * 100}ms` }}>
                      <a href={result.url} target="_blank" rel="noopener noreferrer">{result.docId}</a>
                      <div style={{ fontSize: "12px" }}>{result.chunkedBody}..</div>
                    </div>
                  ))}

                  {searchType === "IMAGE" && (
                    <div className="image-grid">
                      {paginatedResults.map((filename, index) => (
                        <img
                          key={index}
                          src={`${S3_BUCKET_URL}/${filename}.jpg`}
                          alt={`img-${index}`}
                          className="image-item"
                          onClick={() => this.openModal(filename)}
                          onError={(e) => { e.target.style.display = 'none'; }}
                        />
                      ))}
                    </div>
                  )}


                  {totalResults > 0 && (
                    <div className="mt-4 d-flex justify-content-between align-items-center" style={{ fontSize: "13px" }}>
                      <span>
                        Showing {startIndex + 1}–{Math.min(endIndex, totalResults)} of {totalResults} results
                      </span>
                      <div>
                        <Button className="btn-sm me-2" disabled={currentPage === 1} onClick={() => this.scrollToResults(currentPage - 1)}>
                          Prev
                        </Button>
                        <Button className="btn-sm" disabled={endIndex >= totalResults} onClick={() => this.scrollToResults(currentPage + 1)}>
                          Next
                        </Button>
                      </div>
                    </div>
                  )}
                </Col>
              </Row>
            )}

          {showModal && (
            <div className="image-modal" onClick={this.closeModal}>
              <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                <span className="modal-close" onClick={this.closeModal}>&times;</span>

                <button className="modal-nav modal-prev" onClick={this.prevImage}>&lt;</button>
                <img
                  src={`${S3_BUCKET_URL}/${selectedImage}.jpg`}
                  alt="Zoomed"
                />
                <button className="modal-nav modal-next" onClick={this.nextImage}>&gt;</button>

                <div className="modal-actions">
                  <button onClick={() => window.open(`${S3_BUCKET_URL}/${selectedImage}.jpg`, '_blank')}>
                    Download
                  </button>
                  <button onClick={() => this.shareImage(`${S3_BUCKET_URL}/${selectedImage}.jpg`)}>
                    Share
                  </button>
                </div>
              </div>
            </div>
          )}

            
          </div>
        </Container>
      </div>
    );
  }
}

export default App;
