const request = require('supertest');
const app = require('../app');

jest.setTimeout(15000);

describe('GET /query-stem', () => {
  it('returns relevant documents for a valid query', async () => {
    const res = await request(app)
      .get('/query-stem')
      .query({ query: '', optionName: 'tfidf' });
      
  });

  it('handles empty query safely', async () => {
    const res = await request(app).get('/query-stem').query({ query: '   ' });
    expect(res.body.success).toBe(false);
  });
});
