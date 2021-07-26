package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// httpServer powers our application server
type httpServer struct {
	Log *Log
}

// ProduceRequest holds the Record data from an http request. The Record is appended to the httpServer Log.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse holds the Offset of an appended Record. The Offset is returned as a successful response from handleProduce.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest holds the Offset data from an http request. The Offset is used to read from the httpServer Log.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse holds the Record data from a Log in the httpServer. The Record is returned as a successful response from handleConsume.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// handler function for our append/produce method
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	/**
		Decode writes the content of r into our req variable
		req only expects a Record property, aliased as "record" for unmarshalling of json
	**/
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handler function for our read/consume method
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// creates and returns an *httpServer
func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
