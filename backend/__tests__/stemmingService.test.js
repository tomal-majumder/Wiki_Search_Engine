const { stemQuery } = require('../services/stemmingService');

test('stemQuery returns NER-aware, stemmed tokens', async () => {
  const tokens = await stemQuery('Barack Obama visited New York');
  expect(tokens).toContain('obama');
  expect(tokens).toContain('visit');
  expect(tokens).toContain('new york');
});
