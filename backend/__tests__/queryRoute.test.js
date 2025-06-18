const request = require('supertest');
const app = require('../app');

jest.setTimeout(15000);

describe('GET /query-stem', () => {
  it('returns relevant documents for a valid query', async () => {
    const res = await request(app)
      .get('/query-stem')
      .query({ query: 'messi argentina', optionName: 'tfidf' });

    expect(res.statusCode).toBe(200);
    expect(Array.isArray(res.body.textResult)).toBe(true);
    expect(!isNaN(parseFloat(res.body.searchTime))).toBe(true);
  });

  it('handles empty query safely', async () => {
    const res = await request(app).get('/query-stem').query({ query: '   ' });
    expect(res.body.success).toBe(false);
  });
});
