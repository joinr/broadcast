(ns broadcast.core)
;;import Base: getindex, iterate, axes, eachindex, tail, @propagate_inbounds


;;need to look at propogate_inbounds


;; struct LazyBroadcast{F, Args}
;;     f::F
;;     args::Args
;; end


;;ndimensional cart range...
;; [0 0] [10 10]
;; =>
;; {0 10} x {0 10}
;; =>
;; (range 0 10) x (range 0 10)
;; =>
;; [0 1] [5 5]
;; =>
;; (range 0 5)
;; (range 1 5)

(defn card= [l r]
  (if (and (coll? l) (coll? r))
    (= (count l) (count r))
    (and (number? l) (number? r))))

(defn lex< [l r]
  (and (card= l r)
       (== (compare l r) -1)))

(defn lex> [l r]
  (and (card= l r)
       (== (compare l r) 1)))

(defn lex= [l r]
  (and (card= l r)
       (== (compare l r) 0)))

;;since we already have range...
#_(defn combinations [rs]
  
  )

(defmacro cartesian-for [start stop]
  (if (and (coll? start) (coll? stop))
    (let [indices (vec (repeatedly (count start) #(gensym "index")))]
      (let [ascending? (lex< start stop)
            [l r] (if ascending? [start stop] [stop start])]                       
        `(~(if ascending? 'identity 'reverse)
          (for [~@(reduce (fn [acc pair]
                            (conj acc (first pair) (second pair)))
                          []
                          (map-indexed (fn [idx [start stop]]
                                         (let [index (indices idx)]
                                           [index `(range ~start ~stop)]))
                                       (map vector l r)))]
            [~@indices]))))
    (if (lex< start stop)
      `(range ~start ~stop)
      `(reverse (range ~stop ~start)))))

;;workhorse...
(defn random-indices [n]
  (map #(symbol (str "x" %)) (range n)))

(defn index-wrapper [indices]
  (eval `(fn [f#]
           (fn ~'wrapped-fn [acc# ~@indices]
             (f# acc# ~@indices)))))

(defmacro cartesian-reduce
  ([indices acc init start stop first? body]
   (if (seq start)
     (let [l (first start)
           r (first stop)
           idx (first indices)
           inc (if (< l r) 'unchecked-inc  'unchecked-dec)]
       (if (seq (rest start))
         `(loop [~idx ~l
                 acc# ~init]
            (cond (== ~idx ~r) acc#
                  (reduced? acc#) (if ~first? (deref acc#) acc#)
                  :else (recur (~inc ~idx)
                               (cartesian-reduce ~(rest indices)
                                                 ~acc
                                                 acc#
                                              ~(rest start)
                                              ~(rest stop) nil ~body))))
         `(loop [~idx ~l
                 ~acc ~init]
            (cond (== ~idx ~r) ~acc
                  (reduced? ~acc) (if ~first? (deref ~acc) ~acc)
                  :else (recur (~inc ~idx)
                               ~body)))))))
  ([indices acc init start stop body]
   `(cartesian-reduce ~indices ~acc ~init ~start ~stop true ~body))
  ([start stop init body]
   (let [indices (random-indices (count start))]
     `(cartesian-reduce ~indices ~'acc ~init ~start ~stop ~body)))
  ([start stop body]
   `(cartesian-reduce ~start ~stop nil ~body)))

;;encode cartesian indices as vectors.
(deftype cartesian-range [start stop indices
                          ^:unsynchronized-mutable size
                          ^:unsynchronized-mutable expr
                          ]
  clojure.lang.Seqable
  (seq [this]
    (when-not expr
      (set! expr (eval `(fn [] (cartesian-for ~start ~stop)))))
    (expr))
  clojure.core.protocols/CollReduce
  (coll-reduce [coll f]
    (throw (ex-info "not implemented" {:fn 'coll-reduce-2})))
  (coll-reduce [coll f val]
    (eval `(let [f# ~f]
             (cartesian-reduce ~start ~stop ~val (f# ~'acc ~@indices)))))
  clojure.lang.Reversible
  (rseq [this] (seq (cartesian-range. stop start indices size nil))))

(defn cart-range [start stop]
  (cartesian-range. start stop (random-indices (count start)) nil nil))

(defrecord lb [f args])

(defprotocol IBroadcast
  (br-index [obj i]))

(defn br-get-index [x i]
  (if (scalar? x) scalar
      (br-index x i)))

;;loose port...
(defn ntuple [f n]
  (mapv f (range n)))

;;lame NDArray protocol....
;;since we can't use libaries....
(defprotocol IND
  (shape  [obj])
  (rank   [obj])
  (length [obj]))

(def array-types #{(Class/forName "[D")
                   (Class/forName "[F")
                   (Class/forName "[I")})

(defn array? [obj] (array-types (type obj)))  

(defn array-shape [arr]
  (let [dim (alength arr)]
    (if (array? (aget arr 0))
      (mapv alength arr)
      [dim])))

(extend-protocol IND
  (Class/forName "[D")
  (shape  [obj]   (array-shape obj))
  (rank   [obj]   (if (array? (aget obj 0)
                              (alength obj))
  (length [obj]   (reduce * (shape obj))))

;;julia compile-time constant coercer...
(defn val [x] )
(defn scalar? [obj] (number? obj))

(defn ndims [obj]
  (if (scalar? obj) 1
      (rank obj)))

(defn size
  ([obj]
   (if (scalar? obj) 1
       (shape obj)))
  ([obj & dims]
   (if (scalar? obj) 1
       (mapv #(nth obj %) (shape obj)))))

(defn br-get-index [obj i]
  (if (scalar? obj) obj
      (br-index obj i)))

;; br_getindex(scalar, I) = scalar # Scalars no need to index them


;; @propagate_inbounds function br_getindex(A::AbstractArray, I)
;;     idx = ntuple(i-> ifelse(size(A, i) === 1, 1, I[i]), Val(ndims(A)))
;;     return A[CartesianIndex(idx)]
;; end
(extend-protocol IBroadcast  
  ;;double arrays
  (Class/forName "[D")
  (br-index [obj idx]
    (let [^doubles obj obj
          idx   (ntuple (fn [i]
                          (if (== (size obj i)  1)
                            0
                            (nth idx i))) (ndims obj))
          _ (println idx)]
      (apply aget obj idx))))
               
@propagate_inbounds function br_getindex(x::LazyBroadcast, I)
    # this could be a map, but the current map in 1.0 has a perf problem
    return x.f(getindex_arg(x.args, I)...) 
end

getindex_arg(args::Tuple{}, I) = () # recursion ancor
@propagate_inbounds function getindex_arg(args::NTuple{N, Any}, I) where N
    return (br_getindex(args[1], I), getindex_arg(tail(args), I)...)
end

@propagate_inbounds getindex(x::LazyBroadcast, I) = br_getindex(x, Tuple(I))
function materialize!(out::AbstractArray, call_tree::LazyBroadcast)
    # an n-dimensional simd accelerated loop
    @simd for i in CartesianIndices(axes(out)) 
        @inbounds out[i] = call_tree[i]
    end
    return out
end

br_construct(x) = x
function br_construct(x::Expr)
    x.args .= br_construct.(x.args) # apply recursively
    if Meta.isexpr(x, :call) # replace calls to construct LazyBroadcasts
        x = :(LazyBroadcast($(x.args[1]), ($(x.args[2:end]...),)))
    end
    x
end

# macro to enable the syntax @broadcast a + b - sin(c) to construct our type
macro broadcast(call_expr) 
    esc(br_construct(call_expr))
end
