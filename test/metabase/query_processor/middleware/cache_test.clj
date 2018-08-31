(ns metabase.query-processor.middleware.cache-test
  "Tests for the Query Processor cache."
  (:require [expectations :refer :all]
            [metabase.models.query-cache :refer [QueryCache]]
            [metabase.query-processor.middleware.cache :as cache]
            [metabase.test.util :as tu]
            [clojure.tools.logging :as log]
            [toucan.db :as db]))

(def ^:private mock-results
  {:row_count 8
   :status    :completed
   :data      {:rows [[:toucan      71]
                      [:bald-eagle  92]
                      [:hummingbird 11]
                      [:owl         10]
                      [:chicken     69]
                      [:robin       96]
                      [:osprey      72]
                      [:flamingo    70]]}})

(def test-query 
  {:native 
    {
      :query "SELECT * FROM schema1.table1", 
      :template_tags {}}, 
      :type "native", 
      :database 1, 
      :parameters [], 
      :constraints 
      {
        :max-results 10000, 
        :max-results-bare-rows 2000}, 
        :info 
        {
          :executed-by 1, 
          :context 
          :ad-hoc, 
          :card-id nil, 
          :nested? false, 
          :query-type "native"}})

(def ^:private ^:dynamic ^Integer *query-execution-delay-ms* 0)

(defn parse-date [date-string] 
  (new java.sql.Date (.getTime (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd hh:mm") date-string)))
  )

(defn- mock-qp [& _]
  (Thread/sleep *query-execution-delay-ms*)
  mock-results)

(def ^:private maybe-return-cached-results (delay (cache/maybe-return-cached-results mock-qp)))

(defn- clear-cache! [] (db/simple-delete! QueryCache))

(defn- cached? [results]
  (if (:cached results)
    :cached
    :not-cached))

(defn- run-query [& {:as query-kvs}]
  (cached? (@maybe-return-cached-results (merge {:cache_ttl 60, :query :abc} query-kvs))))


;;; -------------------------------------------- tests for is-cacheable? ---------------------------------------------

;; something is-cacheable? if it includes a cach_ttl and the caching setting is enabled
(expect
  (tu/with-temporary-setting-values [enable-query-caching true
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (#'cache/is-cacheable? {:cache_ttl 100})))

(expect
  false
  (tu/with-temporary-setting-values [enable-query-caching false
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (#'cache/is-cacheable? {:cache_ttl 100})))

(expect
  false
  (tu/with-temporary-setting-values [enable-query-caching true
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (#'cache/is-cacheable? {:cache_ttl nil})))


;;; ------------------------------------- redshift state cache ------------------------------------------------------

;; Cached value time was after the last table update, return the cached value
(expect
  1
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  test-query (. (parse-date "2018-08-23 06:00") getTime)))
    (. rsc cancel)
    result
  )))

;; Cached value time was before the last table update, return false
(expect
  0
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  test-query (. (parse-date "2018-08-23 01:00") getTime)))
    (. rsc cancel)
    result
  )))

;; No schema specified, cached value time was after all of the last table updates, return the cached value
(expect
  1
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  (assoc-in test-query [:native :query] "SELECT * FROM table1") (. (parse-date "2018-08-23 06:00") getTime)))
    (. rsc cancel)
    result
  )))

;; No schema specified, Cached value time was before one of the last table update, return false
(expect
  0
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  (assoc-in test-query [:native :query] "SELECT * FROM table1") (. (parse-date "2018-08-23 03:00") getTime)))
    (. rsc cancel)
    result
  )))

;; Unrecognized schema, return false
(expect
  2
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  (assoc-in test-query [:native :query] "SELECT * FROM missingschema.table1")  (. (parse-date "2018-08-23 01:00") getTime)))
    (. rsc cancel)
    result
  )))

;; Unrecognized table in known schema, return false
(expect
  2
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  (assoc-in test-query [:native :query] "SELECT * FROM schema1.missingtable")  (. (parse-date "2018-08-23 01:00") getTime)))
    (. rsc cancel)
    result
  )))

;; Unrecognized table no schema specified, return false
(expect
  2
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false 
                                     redshiftstate-caching-db-num -1 ]
  (let [] 
    (def rsc (new RedshiftStateCache 1 (new DummySetTableLastUpdated { "SCHEMA1" { "TABLE1" (parse-date "2018-08-23 02:00") "TABLE2" (parse-date "2018-08-23 03:00")} 
                                                                        "SCHEMA2" { "TABLE1" (parse-date "2018-08-23 04:00") "TABLE2" (parse-date "2018-08-23 05:00")} })))
    (def result (. rsc shouldReturnCachedResult  (assoc-in test-query [:native :query] "SELECT * FROM missingtable")  (. (parse-date "2018-08-23 01:00") getTime)))
    (. rsc cancel)
    result
  )))

;;; ------------------------------------- results-are-below-max-byte-threshold? --------------------------------------

(expect
  (tu/with-temporary-setting-values [query-caching-max-kb 128
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (#'cache/results-are-below-max-byte-threshold? {:data {:rows [[1 "ABCDEF"]
                                                                  [3 "GHIJKL"]]}})))

(expect
  false
  (tu/with-temporary-setting-values [query-caching-max-kb 1
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (#'cache/results-are-below-max-byte-threshold? {:data {:rows (repeat 500 [1 "ABCDEF"])}})))

;; check that `#'cache/results-are-below-max-byte-threshold?` is lazy and fails fast if the query is over the
;; threshold rather than serializing the entire thing
(expect
  false
  (let [lazy-seq-realized? (atom false)]
    (tu/with-temporary-setting-values [query-caching-max-kb 1
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
      (#'cache/results-are-below-max-byte-threshold? {:data {:rows (lazy-cat (repeat 500 [1 "ABCDEF"])
                                                                             (do (reset! lazy-seq-realized? true)
                                                                                 [2 "GHIJKL"]))}})
      @lazy-seq-realized?)))


;;; ------------------------------------------ End-to-end middleware tests -------------------------------------------

;; if there's nothing in the cache, cached results should *not* be returned
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)))

;; if we run the query twice, the second run should return cached results
(expect
  :cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (run-query)))

;; ...but if the cache entry is past it's TTL, the cached results shouldn't be returned
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query :cache_ttl 1)
    (Thread/sleep 2000)
    (run-query :cache_ttl 1)))

;; if caching is disabled then cache shouldn't be used even if there's something valid in there
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (tu/with-temporary-setting-values [enable-query-caching  false
                                       query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
      (run-query))))


;; check that `query-caching-max-kb` is respected and queries aren't cached if they're past the threshold
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-max-kb  0
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (run-query)))

;; check that `query-caching-max-ttl` is respected. Whenever a new query is cached the cache should evict any entries
;; older that `query-caching-max-ttl`. Set max-ttl to one second, run query `:abc`, then wait two seconds, and run
;; `:def`. This should trigger the cache flush for entries past `:max-ttl`; and the cached entry for `:abc` should be
;; deleted. Running `:abc` a subsequent time should not return cached results
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-max-ttl 1
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (Thread/sleep 2000)
    (run-query, :query :def)
    (run-query)))

;; check that *ignore-cached-results* is respected when returning results...
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (binding [cache/*ignore-cached-results* true]
      (run-query))))

;; ...but if it's set those results should still be cached for next time.
(expect
  :cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 0
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (binding [cache/*ignore-cached-results* true]
      (run-query))
    (run-query)))

;; if the cache takes less than the min TTL to execute, it shouldn't be cached
(expect
  :not-cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 60
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (clear-cache!)
    (run-query)
    (run-query)))

;; ...but if it takes *longer* than the min TTL, it should be cached
(expect
  :cached
  (tu/with-temporary-setting-values [enable-query-caching  true
                                     query-caching-min-ttl 1
                                     enable-redshiftstate-caching false
                                     redshiftstate-caching-db-num -1 ]
    (binding [*query-execution-delay-ms* 1200]
      (clear-cache!)
      (run-query)
      (run-query))))
