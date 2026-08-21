export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { key } = req.query;
  if (!key) {
    return res.status(400).json({ error: 'Pass a submissionKey as ?key=...' });
  }

  const id = process.env.DOFORMS_WEBSERVICE_ID;
  const password = process.env.DOFORMS_PASSWORD;

  if (!id || !password) {
    return res.status(500).json({ error: 'Missing DOFORMS_WEBSERVICE_ID / DOFORMS_PASSWORD env vars' });
  }

  const token = `${id}:${password}`;
  const url = `https://api.mydoforms.com/api/v2/submissions/${encodeURIComponent(key)}`;

  try {
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
    });

    const text = await response.text();

    return res.status(200).json({
      requestedUrl: url,
      doformsStatus: response.status,
      doformsBody: text,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
