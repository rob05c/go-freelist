package get

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

func GetHTTP(url string, client *http.Client) GetterFunc {
	return func() (io.ReadCloser, *CacheObjMetaData, error) {
		fmt.Println("DEBUG GetHTTP fetching from origin!")
		reqTime := time.Now()
		resp, err := client.Get(url) // TODO add cache, IMS, all that magic
		respTime := time.Now()
		if err != nil {
			return nil, &CacheObjMetaData{ReadErr: err}, err // TODO optimize, don't return err twice? Probably not significant.
		}
		// TODO add missing fields
		md := &CacheObjMetaData{
			// ReqHeaders: http.Header{}
			RespHeaders: resp.Header,
			// RespCacheControl: web.CacheControl
			Code:       resp.StatusCode, // TODO fix - what's the difference in this and OriginCode?
			OriginCode: resp.StatusCode,
			// ProxyURL         string
			ReqTime:     reqTime,
			ReqRespTime: respTime,
			// RespRespTime:     time.Time // the origin server's Date time when the object was sent
			// LastModified     time.Time // the origin LastModified if it exists, or Date if it doesn't
			// Size             uint64
		}
		return resp.Body, md, nil
	}
}

// GetHTTPMock pretends to get from HTTP, but actually returns the hard-coded body from example.com.
// Exists for testing.
func GetHTTPMock(url string, client *http.Client) GetterFunc {
	return func() (io.ReadCloser, *CacheObjMetaData, error) {
		fmt.Println("DEBUG GetHTTPMock fetching from origin! (not really)")
		reqTime := time.Now()

		respStatusCode := 200
		respBody := ioutil.NopCloser(bytes.NewBufferString(httpMockResp))
		respTime := time.Now()

		// TODO add missing fields
		md := &CacheObjMetaData{
			// ReqHeaders: http.Header{}
			// RespHeaders: resp.Header,
			// RespCacheControl: web.CacheControl
			Code:       respStatusCode, // TODO fix - what's the difference in this and OriginCode?
			OriginCode: respStatusCode,
			// ProxyURL         string
			ReqTime:     reqTime,
			ReqRespTime: respTime,
			// RespRespTime:     time.Time // the origin server's Date time when the object was sent
			// LastModified     time.Time // the origin LastModified if it exists, or Date if it doesn't
			// Size             uint64
		}
		return respBody, md, nil
	}
}

var httpMockResp = `<!doctype html>
<html>
<head>
    <title>Example Domain</title>

    <meta charset="utf-8" />
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style type="text/css">
    body {
        background-color: #f0f0f2;
        margin: 0;
        padding: 0;
        font-family: -apple-system, system-ui, BlinkMacSystemFont, "Segoe UI", "Open Sans", "Helvetica Neue", Helvetica,
 Arial, sans-serif;
    }
    div {
        width: 600px;
        margin: 5em auto;
        padding: 2em;
        background-color: #fdfdff;
        border-radius: 0.5em;
        box-shadow: 2px 3px 7px 2px rgba(0,0,0,0.02);
    }
    a:link, a:visited {
        color: #38488f;
        text-decoration: none;
    }
    @media (max-width: 700px) {
        div {
            margin: 0 auto;
            width: auto;
        }
    }
    </style>
</head>

<body>
<div>
    <h1>Example Domain</h1>
    <p>This domain is for use in illustrative examples in documents. You may use this
    domain in literature without prior coordination or asking for permission.</p>
    <p><a href="https://www.iana.org/domains/example">More information...</a></p>
</div>
</body>
</html>
`
