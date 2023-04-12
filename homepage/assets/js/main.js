/******/ (() => {
  // webpackBootstrap
  /******/ "use strict";
  /******/ var __webpack_modules__ = {
    /***/ "./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js":
      /*!*****************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js ***!
  \*****************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ BehaviorSubject: () =>
            /* binding */ BehaviorSubject,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");
        /* harmony import */ var _Subject__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./Subject */ "./node_modules/rxjs/dist/esm5/internal/Subject.js"
          );

        var BehaviorSubject = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(
            BehaviorSubject,
            _super
          );
          function BehaviorSubject(_value) {
            var _this = _super.call(this) || this;
            _this._value = _value;
            return _this;
          }
          Object.defineProperty(BehaviorSubject.prototype, "value", {
            get: function () {
              return this.getValue();
            },
            enumerable: false,
            configurable: true,
          });
          BehaviorSubject.prototype._subscribe = function (subscriber) {
            var subscription = _super.prototype._subscribe.call(
              this,
              subscriber
            );
            !subscription.closed && subscriber.next(this._value);
            return subscription;
          };
          BehaviorSubject.prototype.getValue = function () {
            var _a = this,
              hasError = _a.hasError,
              thrownError = _a.thrownError,
              _value = _a._value;
            if (hasError) {
              throw thrownError;
            }
            this._throwIfClosed();
            return _value;
          };
          BehaviorSubject.prototype.next = function (value) {
            _super.prototype.next.call(this, (this._value = value));
          };
          return BehaviorSubject;
        })(_Subject__WEBPACK_IMPORTED_MODULE_1__.Subject);

        //# sourceMappingURL=BehaviorSubject.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js":
      /*!***********************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js ***!
  \***********************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ COMPLETE_NOTIFICATION: () =>
            /* binding */ COMPLETE_NOTIFICATION,
          /* harmony export */ createNotification: () =>
            /* binding */ createNotification,
          /* harmony export */ errorNotification: () =>
            /* binding */ errorNotification,
          /* harmony export */ nextNotification: () =>
            /* binding */ nextNotification,
          /* harmony export */
        });
        var COMPLETE_NOTIFICATION = (function () {
          return createNotification("C", undefined, undefined);
        })();
        function errorNotification(error) {
          return createNotification("E", undefined, error);
        }
        function nextNotification(value) {
          return createNotification("N", value, undefined);
        }
        function createNotification(kind, value, error) {
          return {
            kind: kind,
            value: value,
            error: error,
          };
        }
        //# sourceMappingURL=NotificationFactories.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/Observable.js":
      /*!************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/Observable.js ***!
  \************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ Observable: () => /* binding */ Observable,
          /* harmony export */
        });
        /* harmony import */ var _Subscriber__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./Subscriber */ "./node_modules/rxjs/dist/esm5/internal/Subscriber.js"
          );
        /* harmony import */ var _Subscription__WEBPACK_IMPORTED_MODULE_6__ =
          __webpack_require__(
            /*! ./Subscription */ "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
        /* harmony import */ var _symbol_observable__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ./symbol/observable */ "./node_modules/rxjs/dist/esm5/internal/symbol/observable.js"
          );
        /* harmony import */ var _util_pipe__WEBPACK_IMPORTED_MODULE_3__ =
          __webpack_require__(
            /*! ./util/pipe */ "./node_modules/rxjs/dist/esm5/internal/util/pipe.js"
          );
        /* harmony import */ var _config__WEBPACK_IMPORTED_MODULE_4__ =
          __webpack_require__(
            /*! ./config */ "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
        /* harmony import */ var _util_isFunction__WEBPACK_IMPORTED_MODULE_5__ =
          __webpack_require__(
            /*! ./util/isFunction */ "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
        /* harmony import */ var _util_errorContext__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./util/errorContext */ "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );

        var Observable = (function () {
          function Observable(subscribe) {
            if (subscribe) {
              this._subscribe = subscribe;
            }
          }
          Observable.prototype.lift = function (operator) {
            var observable = new Observable();
            observable.source = this;
            observable.operator = operator;
            return observable;
          };
          Observable.prototype.subscribe = function (
            observerOrNext,
            error,
            complete
          ) {
            var _this = this;
            var subscriber = isSubscriber(observerOrNext)
              ? observerOrNext
              : new _Subscriber__WEBPACK_IMPORTED_MODULE_0__.SafeSubscriber(
                  observerOrNext,
                  error,
                  complete
                );
            (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_1__.errorContext)(
              function () {
                var _a = _this,
                  operator = _a.operator,
                  source = _a.source;
                subscriber.add(
                  operator
                    ? operator.call(subscriber, source)
                    : source
                    ? _this._subscribe(subscriber)
                    : _this._trySubscribe(subscriber)
                );
              }
            );
            return subscriber;
          };
          Observable.prototype._trySubscribe = function (sink) {
            try {
              return this._subscribe(sink);
            } catch (err) {
              sink.error(err);
            }
          };
          Observable.prototype.forEach = function (next, promiseCtor) {
            var _this = this;
            promiseCtor = getPromiseCtor(promiseCtor);
            return new promiseCtor(function (resolve, reject) {
              var subscriber =
                new _Subscriber__WEBPACK_IMPORTED_MODULE_0__.SafeSubscriber({
                  next: function (value) {
                    try {
                      next(value);
                    } catch (err) {
                      reject(err);
                      subscriber.unsubscribe();
                    }
                  },
                  error: reject,
                  complete: resolve,
                });
              _this.subscribe(subscriber);
            });
          };
          Observable.prototype._subscribe = function (subscriber) {
            var _a;
            return (_a = this.source) === null || _a === void 0
              ? void 0
              : _a.subscribe(subscriber);
          };
          Observable.prototype[
            _symbol_observable__WEBPACK_IMPORTED_MODULE_2__.observable
          ] = function () {
            return this;
          };
          Observable.prototype.pipe = function () {
            var operations = [];
            for (var _i = 0; _i < arguments.length; _i++) {
              operations[_i] = arguments[_i];
            }
            return (0, _util_pipe__WEBPACK_IMPORTED_MODULE_3__.pipeFromArray)(
              operations
            )(this);
          };
          Observable.prototype.toPromise = function (promiseCtor) {
            var _this = this;
            promiseCtor = getPromiseCtor(promiseCtor);
            return new promiseCtor(function (resolve, reject) {
              var value;
              _this.subscribe(
                function (x) {
                  return (value = x);
                },
                function (err) {
                  return reject(err);
                },
                function () {
                  return resolve(value);
                }
              );
            });
          };
          Observable.create = function (subscribe) {
            return new Observable(subscribe);
          };
          return Observable;
        })();

        function getPromiseCtor(promiseCtor) {
          var _a;
          return (_a =
            promiseCtor !== null && promiseCtor !== void 0
              ? promiseCtor
              : _config__WEBPACK_IMPORTED_MODULE_4__.config.Promise) !== null &&
            _a !== void 0
            ? _a
            : Promise;
        }
        function isObserver(value) {
          return (
            value &&
            (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(
              value.next
            ) &&
            (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(
              value.error
            ) &&
            (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(
              value.complete
            )
          );
        }
        function isSubscriber(value) {
          return (
            (value &&
              value instanceof
                _Subscriber__WEBPACK_IMPORTED_MODULE_0__.Subscriber) ||
            (isObserver(value) &&
              (0, _Subscription__WEBPACK_IMPORTED_MODULE_6__.isSubscription)(
                value
              ))
          );
        }
        //# sourceMappingURL=Observable.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/Subject.js":
      /*!*********************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/Subject.js ***!
  \*********************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ AnonymousSubject: () =>
            /* binding */ AnonymousSubject,
          /* harmony export */ Subject: () => /* binding */ Subject,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");
        /* harmony import */ var _Observable__WEBPACK_IMPORTED_MODULE_5__ =
          __webpack_require__(
            /*! ./Observable */ "./node_modules/rxjs/dist/esm5/internal/Observable.js"
          );
        /* harmony import */ var _Subscription__WEBPACK_IMPORTED_MODULE_3__ =
          __webpack_require__(
            /*! ./Subscription */ "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
        /* harmony import */ var _util_ObjectUnsubscribedError__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./util/ObjectUnsubscribedError */ "./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js"
          );
        /* harmony import */ var _util_arrRemove__WEBPACK_IMPORTED_MODULE_4__ =
          __webpack_require__(
            /*! ./util/arrRemove */ "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js"
          );
        /* harmony import */ var _util_errorContext__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ./util/errorContext */ "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );

        var Subject = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(Subject, _super);
          function Subject() {
            var _this = _super.call(this) || this;
            _this.closed = false;
            _this.currentObservers = null;
            _this.observers = [];
            _this.isStopped = false;
            _this.hasError = false;
            _this.thrownError = null;
            return _this;
          }
          Subject.prototype.lift = function (operator) {
            var subject = new AnonymousSubject(this, this);
            subject.operator = operator;
            return subject;
          };
          Subject.prototype._throwIfClosed = function () {
            if (this.closed) {
              throw new _util_ObjectUnsubscribedError__WEBPACK_IMPORTED_MODULE_1__.ObjectUnsubscribedError();
            }
          };
          Subject.prototype.next = function (value) {
            var _this = this;
            (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(
              function () {
                var e_1, _a;
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  if (!_this.currentObservers) {
                    _this.currentObservers = Array.from(_this.observers);
                  }
                  try {
                    for (
                      var _b = (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(
                          _this.currentObservers
                        ),
                        _c = _b.next();
                      !_c.done;
                      _c = _b.next()
                    ) {
                      var observer = _c.value;
                      observer.next(value);
                    }
                  } catch (e_1_1) {
                    e_1 = { error: e_1_1 };
                  } finally {
                    try {
                      if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                    } finally {
                      if (e_1) throw e_1.error;
                    }
                  }
                }
              }
            );
          };
          Subject.prototype.error = function (err) {
            var _this = this;
            (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(
              function () {
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  _this.hasError = _this.isStopped = true;
                  _this.thrownError = err;
                  var observers = _this.observers;
                  while (observers.length) {
                    observers.shift().error(err);
                  }
                }
              }
            );
          };
          Subject.prototype.complete = function () {
            var _this = this;
            (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(
              function () {
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  _this.isStopped = true;
                  var observers = _this.observers;
                  while (observers.length) {
                    observers.shift().complete();
                  }
                }
              }
            );
          };
          Subject.prototype.unsubscribe = function () {
            this.isStopped = this.closed = true;
            this.observers = this.currentObservers = null;
          };
          Object.defineProperty(Subject.prototype, "observed", {
            get: function () {
              var _a;
              return (
                ((_a = this.observers) === null || _a === void 0
                  ? void 0
                  : _a.length) > 0
              );
            },
            enumerable: false,
            configurable: true,
          });
          Subject.prototype._trySubscribe = function (subscriber) {
            this._throwIfClosed();
            return _super.prototype._trySubscribe.call(this, subscriber);
          };
          Subject.prototype._subscribe = function (subscriber) {
            this._throwIfClosed();
            this._checkFinalizedStatuses(subscriber);
            return this._innerSubscribe(subscriber);
          };
          Subject.prototype._innerSubscribe = function (subscriber) {
            var _this = this;
            var _a = this,
              hasError = _a.hasError,
              isStopped = _a.isStopped,
              observers = _a.observers;
            if (hasError || isStopped) {
              return _Subscription__WEBPACK_IMPORTED_MODULE_3__.EMPTY_SUBSCRIPTION;
            }
            this.currentObservers = null;
            observers.push(subscriber);
            return new _Subscription__WEBPACK_IMPORTED_MODULE_3__.Subscription(
              function () {
                _this.currentObservers = null;
                (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_4__.arrRemove)(
                  observers,
                  subscriber
                );
              }
            );
          };
          Subject.prototype._checkFinalizedStatuses = function (subscriber) {
            var _a = this,
              hasError = _a.hasError,
              thrownError = _a.thrownError,
              isStopped = _a.isStopped;
            if (hasError) {
              subscriber.error(thrownError);
            } else if (isStopped) {
              subscriber.complete();
            }
          };
          Subject.prototype.asObservable = function () {
            var observable =
              new _Observable__WEBPACK_IMPORTED_MODULE_5__.Observable();
            observable.source = this;
            return observable;
          };
          Subject.create = function (destination, source) {
            return new AnonymousSubject(destination, source);
          };
          return Subject;
        })(_Observable__WEBPACK_IMPORTED_MODULE_5__.Observable);

        var AnonymousSubject = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(
            AnonymousSubject,
            _super
          );
          function AnonymousSubject(destination, source) {
            var _this = _super.call(this) || this;
            _this.destination = destination;
            _this.source = source;
            return _this;
          }
          AnonymousSubject.prototype.next = function (value) {
            var _a, _b;
            (_b =
              (_a = this.destination) === null || _a === void 0
                ? void 0
                : _a.next) === null || _b === void 0
              ? void 0
              : _b.call(_a, value);
          };
          AnonymousSubject.prototype.error = function (err) {
            var _a, _b;
            (_b =
              (_a = this.destination) === null || _a === void 0
                ? void 0
                : _a.error) === null || _b === void 0
              ? void 0
              : _b.call(_a, err);
          };
          AnonymousSubject.prototype.complete = function () {
            var _a, _b;
            (_b =
              (_a = this.destination) === null || _a === void 0
                ? void 0
                : _a.complete) === null || _b === void 0
              ? void 0
              : _b.call(_a);
          };
          AnonymousSubject.prototype._subscribe = function (subscriber) {
            var _a, _b;
            return (_b =
              (_a = this.source) === null || _a === void 0
                ? void 0
                : _a.subscribe(subscriber)) !== null && _b !== void 0
              ? _b
              : _Subscription__WEBPACK_IMPORTED_MODULE_3__.EMPTY_SUBSCRIPTION;
          };
          return AnonymousSubject;
        })(Subject);

        //# sourceMappingURL=Subject.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/Subscriber.js":
      /*!************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/Subscriber.js ***!
  \************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ EMPTY_OBSERVER: () =>
            /* binding */ EMPTY_OBSERVER,
          /* harmony export */ SafeSubscriber: () =>
            /* binding */ SafeSubscriber,
          /* harmony export */ Subscriber: () => /* binding */ Subscriber,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");
        /* harmony import */ var _util_isFunction__WEBPACK_IMPORTED_MODULE_3__ =
          __webpack_require__(
            /*! ./util/isFunction */ "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
        /* harmony import */ var _Subscription__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./Subscription */ "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
        /* harmony import */ var _config__WEBPACK_IMPORTED_MODULE_4__ =
          __webpack_require__(
            /*! ./config */ "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
        /* harmony import */ var _util_reportUnhandledError__WEBPACK_IMPORTED_MODULE_6__ =
          __webpack_require__(
            /*! ./util/reportUnhandledError */ "./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js"
          );
        /* harmony import */ var _util_noop__WEBPACK_IMPORTED_MODULE_8__ =
          __webpack_require__(
            /*! ./util/noop */ "./node_modules/rxjs/dist/esm5/internal/util/noop.js"
          );
        /* harmony import */ var _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ./NotificationFactories */ "./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js"
          );
        /* harmony import */ var _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_7__ =
          __webpack_require__(
            /*! ./scheduler/timeoutProvider */ "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js"
          );
        /* harmony import */ var _util_errorContext__WEBPACK_IMPORTED_MODULE_5__ =
          __webpack_require__(
            /*! ./util/errorContext */ "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );

        var Subscriber = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(Subscriber, _super);
          function Subscriber(destination) {
            var _this = _super.call(this) || this;
            _this.isStopped = false;
            if (destination) {
              _this.destination = destination;
              if (
                (0, _Subscription__WEBPACK_IMPORTED_MODULE_1__.isSubscription)(
                  destination
                )
              ) {
                destination.add(_this);
              }
            } else {
              _this.destination = EMPTY_OBSERVER;
            }
            return _this;
          }
          Subscriber.create = function (next, error, complete) {
            return new SafeSubscriber(next, error, complete);
          };
          Subscriber.prototype.next = function (value) {
            if (this.isStopped) {
              handleStoppedNotification(
                (0,
                _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.nextNotification)(
                  value
                ),
                this
              );
            } else {
              this._next(value);
            }
          };
          Subscriber.prototype.error = function (err) {
            if (this.isStopped) {
              handleStoppedNotification(
                (0,
                _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.errorNotification)(
                  err
                ),
                this
              );
            } else {
              this.isStopped = true;
              this._error(err);
            }
          };
          Subscriber.prototype.complete = function () {
            if (this.isStopped) {
              handleStoppedNotification(
                _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.COMPLETE_NOTIFICATION,
                this
              );
            } else {
              this.isStopped = true;
              this._complete();
            }
          };
          Subscriber.prototype.unsubscribe = function () {
            if (!this.closed) {
              this.isStopped = true;
              _super.prototype.unsubscribe.call(this);
              this.destination = null;
            }
          };
          Subscriber.prototype._next = function (value) {
            this.destination.next(value);
          };
          Subscriber.prototype._error = function (err) {
            try {
              this.destination.error(err);
            } finally {
              this.unsubscribe();
            }
          };
          Subscriber.prototype._complete = function () {
            try {
              this.destination.complete();
            } finally {
              this.unsubscribe();
            }
          };
          return Subscriber;
        })(_Subscription__WEBPACK_IMPORTED_MODULE_1__.Subscription);

        var _bind = Function.prototype.bind;
        function bind(fn, thisArg) {
          return _bind.call(fn, thisArg);
        }
        var ConsumerObserver = (function () {
          function ConsumerObserver(partialObserver) {
            this.partialObserver = partialObserver;
          }
          ConsumerObserver.prototype.next = function (value) {
            var partialObserver = this.partialObserver;
            if (partialObserver.next) {
              try {
                partialObserver.next(value);
              } catch (error) {
                handleUnhandledError(error);
              }
            }
          };
          ConsumerObserver.prototype.error = function (err) {
            var partialObserver = this.partialObserver;
            if (partialObserver.error) {
              try {
                partialObserver.error(err);
              } catch (error) {
                handleUnhandledError(error);
              }
            } else {
              handleUnhandledError(err);
            }
          };
          ConsumerObserver.prototype.complete = function () {
            var partialObserver = this.partialObserver;
            if (partialObserver.complete) {
              try {
                partialObserver.complete();
              } catch (error) {
                handleUnhandledError(error);
              }
            }
          };
          return ConsumerObserver;
        })();
        var SafeSubscriber = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(
            SafeSubscriber,
            _super
          );
          function SafeSubscriber(observerOrNext, error, complete) {
            var _this = _super.call(this) || this;
            var partialObserver;
            if (
              (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_3__.isFunction)(
                observerOrNext
              ) ||
              !observerOrNext
            ) {
              partialObserver = {
                next:
                  observerOrNext !== null && observerOrNext !== void 0
                    ? observerOrNext
                    : undefined,
                error: error !== null && error !== void 0 ? error : undefined,
                complete:
                  complete !== null && complete !== void 0
                    ? complete
                    : undefined,
              };
            } else {
              var context_1;
              if (
                _this &&
                _config__WEBPACK_IMPORTED_MODULE_4__.config
                  .useDeprecatedNextContext
              ) {
                context_1 = Object.create(observerOrNext);
                context_1.unsubscribe = function () {
                  return _this.unsubscribe();
                };
                partialObserver = {
                  next:
                    observerOrNext.next && bind(observerOrNext.next, context_1),
                  error:
                    observerOrNext.error &&
                    bind(observerOrNext.error, context_1),
                  complete:
                    observerOrNext.complete &&
                    bind(observerOrNext.complete, context_1),
                };
              } else {
                partialObserver = observerOrNext;
              }
            }
            _this.destination = new ConsumerObserver(partialObserver);
            return _this;
          }
          return SafeSubscriber;
        })(Subscriber);

        function handleUnhandledError(error) {
          if (
            _config__WEBPACK_IMPORTED_MODULE_4__.config
              .useDeprecatedSynchronousErrorHandling
          ) {
            (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_5__.captureError)(
              error
            );
          } else {
            (0,
            _util_reportUnhandledError__WEBPACK_IMPORTED_MODULE_6__.reportUnhandledError)(
              error
            );
          }
        }
        function defaultErrorHandler(err) {
          throw err;
        }
        function handleStoppedNotification(notification, subscriber) {
          var onStoppedNotification =
            _config__WEBPACK_IMPORTED_MODULE_4__.config.onStoppedNotification;
          onStoppedNotification &&
            _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_7__.timeoutProvider.setTimeout(
              function () {
                return onStoppedNotification(notification, subscriber);
              }
            );
        }
        var EMPTY_OBSERVER = {
          closed: true,
          next: _util_noop__WEBPACK_IMPORTED_MODULE_8__.noop,
          error: defaultErrorHandler,
          complete: _util_noop__WEBPACK_IMPORTED_MODULE_8__.noop,
        };
        //# sourceMappingURL=Subscriber.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/Subscription.js":
      /*!**************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/Subscription.js ***!
  \**************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ EMPTY_SUBSCRIPTION: () =>
            /* binding */ EMPTY_SUBSCRIPTION,
          /* harmony export */ Subscription: () => /* binding */ Subscription,
          /* harmony export */ isSubscription: () =>
            /* binding */ isSubscription,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");
        /* harmony import */ var _util_isFunction__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./util/isFunction */ "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
        /* harmony import */ var _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ./util/UnsubscriptionError */ "./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js"
          );
        /* harmony import */ var _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__ =
          __webpack_require__(
            /*! ./util/arrRemove */ "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js"
          );

        var Subscription = (function () {
          function Subscription(initialTeardown) {
            this.initialTeardown = initialTeardown;
            this.closed = false;
            this._parentage = null;
            this._finalizers = null;
          }
          Subscription.prototype.unsubscribe = function () {
            var e_1, _a, e_2, _b;
            var errors;
            if (!this.closed) {
              this.closed = true;
              var _parentage = this._parentage;
              if (_parentage) {
                this._parentage = null;
                if (Array.isArray(_parentage)) {
                  try {
                    for (
                      var _parentage_1 = (0,
                        tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(
                          _parentage
                        ),
                        _parentage_1_1 = _parentage_1.next();
                      !_parentage_1_1.done;
                      _parentage_1_1 = _parentage_1.next()
                    ) {
                      var parent_1 = _parentage_1_1.value;
                      parent_1.remove(this);
                    }
                  } catch (e_1_1) {
                    e_1 = { error: e_1_1 };
                  } finally {
                    try {
                      if (
                        _parentage_1_1 &&
                        !_parentage_1_1.done &&
                        (_a = _parentage_1.return)
                      )
                        _a.call(_parentage_1);
                    } finally {
                      if (e_1) throw e_1.error;
                    }
                  }
                } else {
                  _parentage.remove(this);
                }
              }
              var initialFinalizer = this.initialTeardown;
              if (
                (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(
                  initialFinalizer
                )
              ) {
                try {
                  initialFinalizer();
                } catch (e) {
                  errors =
                    e instanceof
                    _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError
                      ? e.errors
                      : [e];
                }
              }
              var _finalizers = this._finalizers;
              if (_finalizers) {
                this._finalizers = null;
                try {
                  for (
                    var _finalizers_1 = (0,
                      tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(_finalizers),
                      _finalizers_1_1 = _finalizers_1.next();
                    !_finalizers_1_1.done;
                    _finalizers_1_1 = _finalizers_1.next()
                  ) {
                    var finalizer = _finalizers_1_1.value;
                    try {
                      execFinalizer(finalizer);
                    } catch (err) {
                      errors =
                        errors !== null && errors !== void 0 ? errors : [];
                      if (
                        err instanceof
                        _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError
                      ) {
                        errors = (0,
                        tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)(
                          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)(
                            [],
                            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(
                              errors
                            )
                          ),
                          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(
                            err.errors
                          )
                        );
                      } else {
                        errors.push(err);
                      }
                    }
                  }
                } catch (e_2_1) {
                  e_2 = { error: e_2_1 };
                } finally {
                  try {
                    if (
                      _finalizers_1_1 &&
                      !_finalizers_1_1.done &&
                      (_b = _finalizers_1.return)
                    )
                      _b.call(_finalizers_1);
                  } finally {
                    if (e_2) throw e_2.error;
                  }
                }
              }
              if (errors) {
                throw new _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError(
                  errors
                );
              }
            }
          };
          Subscription.prototype.add = function (teardown) {
            var _a;
            if (teardown && teardown !== this) {
              if (this.closed) {
                execFinalizer(teardown);
              } else {
                if (teardown instanceof Subscription) {
                  if (teardown.closed || teardown._hasParent(this)) {
                    return;
                  }
                  teardown._addParent(this);
                }
                (this._finalizers =
                  (_a = this._finalizers) !== null && _a !== void 0
                    ? _a
                    : []).push(teardown);
              }
            }
          };
          Subscription.prototype._hasParent = function (parent) {
            var _parentage = this._parentage;
            return (
              _parentage === parent ||
              (Array.isArray(_parentage) && _parentage.includes(parent))
            );
          };
          Subscription.prototype._addParent = function (parent) {
            var _parentage = this._parentage;
            this._parentage = Array.isArray(_parentage)
              ? (_parentage.push(parent), _parentage)
              : _parentage
              ? [_parentage, parent]
              : parent;
          };
          Subscription.prototype._removeParent = function (parent) {
            var _parentage = this._parentage;
            if (_parentage === parent) {
              this._parentage = null;
            } else if (Array.isArray(_parentage)) {
              (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__.arrRemove)(
                _parentage,
                parent
              );
            }
          };
          Subscription.prototype.remove = function (teardown) {
            var _finalizers = this._finalizers;
            _finalizers &&
              (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__.arrRemove)(
                _finalizers,
                teardown
              );
            if (teardown instanceof Subscription) {
              teardown._removeParent(this);
            }
          };
          Subscription.EMPTY = (function () {
            var empty = new Subscription();
            empty.closed = true;
            return empty;
          })();
          return Subscription;
        })();

        var EMPTY_SUBSCRIPTION = Subscription.EMPTY;
        function isSubscription(value) {
          return (
            value instanceof Subscription ||
            (value &&
              "closed" in value &&
              (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(
                value.remove
              ) &&
              (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(
                value.add
              ) &&
              (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(
                value.unsubscribe
              ))
          );
        }
        function execFinalizer(finalizer) {
          if (
            (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(
              finalizer
            )
          ) {
            finalizer();
          } else {
            finalizer.unsubscribe();
          }
        }
        //# sourceMappingURL=Subscription.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/config.js":
      /*!********************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/config.js ***!
  \********************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ config: () => /* binding */ config,
          /* harmony export */
        });
        var config = {
          onUnhandledError: null,
          onStoppedNotification: null,
          Promise: undefined,
          useDeprecatedSynchronousErrorHandling: false,
          useDeprecatedNextContext: false,
        };
        //# sourceMappingURL=config.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js":
      /*!******************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js ***!
  \******************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ OperatorSubscriber: () =>
            /* binding */ OperatorSubscriber,
          /* harmony export */ createOperatorSubscriber: () =>
            /* binding */ createOperatorSubscriber,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");
        /* harmony import */ var _Subscriber__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ../Subscriber */ "./node_modules/rxjs/dist/esm5/internal/Subscriber.js"
          );

        function createOperatorSubscriber(
          destination,
          onNext,
          onComplete,
          onError,
          onFinalize
        ) {
          return new OperatorSubscriber(
            destination,
            onNext,
            onComplete,
            onError,
            onFinalize
          );
        }
        var OperatorSubscriber = (function (_super) {
          (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(
            OperatorSubscriber,
            _super
          );
          function OperatorSubscriber(
            destination,
            onNext,
            onComplete,
            onError,
            onFinalize,
            shouldUnsubscribe
          ) {
            var _this = _super.call(this, destination) || this;
            _this.onFinalize = onFinalize;
            _this.shouldUnsubscribe = shouldUnsubscribe;
            _this._next = onNext
              ? function (value) {
                  try {
                    onNext(value);
                  } catch (err) {
                    destination.error(err);
                  }
                }
              : _super.prototype._next;
            _this._error = onError
              ? function (err) {
                  try {
                    onError(err);
                  } catch (err) {
                    destination.error(err);
                  } finally {
                    this.unsubscribe();
                  }
                }
              : _super.prototype._error;
            _this._complete = onComplete
              ? function () {
                  try {
                    onComplete();
                  } catch (err) {
                    destination.error(err);
                  } finally {
                    this.unsubscribe();
                  }
                }
              : _super.prototype._complete;
            return _this;
          }
          OperatorSubscriber.prototype.unsubscribe = function () {
            var _a;
            if (!this.shouldUnsubscribe || this.shouldUnsubscribe()) {
              var closed_1 = this.closed;
              _super.prototype.unsubscribe.call(this);
              !closed_1 &&
                ((_a = this.onFinalize) === null || _a === void 0
                  ? void 0
                  : _a.call(this));
            }
          };
          return OperatorSubscriber;
        })(_Subscriber__WEBPACK_IMPORTED_MODULE_1__.Subscriber);

        //# sourceMappingURL=OperatorSubscriber.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js":
      /*!********************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js ***!
  \********************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ distinctUntilChanged: () =>
            /* binding */ distinctUntilChanged,
          /* harmony export */
        });
        /* harmony import */ var _util_identity__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ../util/identity */ "./node_modules/rxjs/dist/esm5/internal/util/identity.js"
          );
        /* harmony import */ var _util_lift__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ../util/lift */ "./node_modules/rxjs/dist/esm5/internal/util/lift.js"
          );
        /* harmony import */ var _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ./OperatorSubscriber */ "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js"
          );

        function distinctUntilChanged(comparator, keySelector) {
          if (keySelector === void 0) {
            keySelector = _util_identity__WEBPACK_IMPORTED_MODULE_0__.identity;
          }
          comparator =
            comparator !== null && comparator !== void 0
              ? comparator
              : defaultCompare;
          return (0, _util_lift__WEBPACK_IMPORTED_MODULE_1__.operate)(function (
            source,
            subscriber
          ) {
            var previousKey;
            var first = true;
            source.subscribe(
              (0,
              _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_2__.createOperatorSubscriber)(
                subscriber,
                function (value) {
                  var currentKey = keySelector(value);
                  if (first || !comparator(previousKey, currentKey)) {
                    first = false;
                    previousKey = currentKey;
                    subscriber.next(value);
                  }
                }
              )
            );
          });
        }
        function defaultCompare(a, b) {
          return a === b;
        }
        //# sourceMappingURL=distinctUntilChanged.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/operators/map.js":
      /*!***************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/operators/map.js ***!
  \***************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ map: () => /* binding */ map,
          /* harmony export */
        });
        /* harmony import */ var _util_lift__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ../util/lift */ "./node_modules/rxjs/dist/esm5/internal/util/lift.js"
          );
        /* harmony import */ var _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./OperatorSubscriber */ "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js"
          );

        function map(project, thisArg) {
          return (0, _util_lift__WEBPACK_IMPORTED_MODULE_0__.operate)(function (
            source,
            subscriber
          ) {
            var index = 0;
            source.subscribe(
              (0,
              _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_1__.createOperatorSubscriber)(
                subscriber,
                function (value) {
                  subscriber.next(project.call(thisArg, value, index++));
                }
              )
            );
          });
        }
        //# sourceMappingURL=map.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js":
      /*!***************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js ***!
  \***************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ timeoutProvider: () =>
            /* binding */ timeoutProvider,
          /* harmony export */
        });
        /* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! tslib */ "./node_modules/tslib/tslib.es6.js");

        var timeoutProvider = {
          setTimeout: function (handler, timeout) {
            var args = [];
            for (var _i = 2; _i < arguments.length; _i++) {
              args[_i - 2] = arguments[_i];
            }
            var delegate = timeoutProvider.delegate;
            if (
              delegate === null || delegate === void 0
                ? void 0
                : delegate.setTimeout
            ) {
              return delegate.setTimeout.apply(
                delegate,
                (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)(
                  [handler, timeout],
                  (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(args)
                )
              );
            }
            return setTimeout.apply(
              void 0,
              (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)(
                [handler, timeout],
                (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(args)
              )
            );
          },
          clearTimeout: function (handle) {
            var delegate = timeoutProvider.delegate;
            return (
              (delegate === null || delegate === void 0
                ? void 0
                : delegate.clearTimeout) || clearTimeout
            )(handle);
          },
          delegate: undefined,
        };
        //# sourceMappingURL=timeoutProvider.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/symbol/observable.js":
      /*!*******************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/symbol/observable.js ***!
  \*******************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ observable: () => /* binding */ observable,
          /* harmony export */
        });
        var observable = (function () {
          return (
            (typeof Symbol === "function" && Symbol.observable) ||
            "@@observable"
          );
        })();
        //# sourceMappingURL=observable.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js":
      /*!******************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js ***!
  \******************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ ObjectUnsubscribedError: () =>
            /* binding */ ObjectUnsubscribedError,
          /* harmony export */
        });
        /* harmony import */ var _createErrorClass__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./createErrorClass */ "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js"
          );

        var ObjectUnsubscribedError = (0,
        _createErrorClass__WEBPACK_IMPORTED_MODULE_0__.createErrorClass)(
          function (_super) {
            return function ObjectUnsubscribedErrorImpl() {
              _super(this);
              this.name = "ObjectUnsubscribedError";
              this.message = "object unsubscribed";
            };
          }
        );
        //# sourceMappingURL=ObjectUnsubscribedError.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js":
      /*!**************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js ***!
  \**************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ UnsubscriptionError: () =>
            /* binding */ UnsubscriptionError,
          /* harmony export */
        });
        /* harmony import */ var _createErrorClass__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./createErrorClass */ "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js"
          );

        var UnsubscriptionError = (0,
        _createErrorClass__WEBPACK_IMPORTED_MODULE_0__.createErrorClass)(
          function (_super) {
            return function UnsubscriptionErrorImpl(errors) {
              _super(this);
              this.message = errors
                ? errors.length +
                  " errors occurred during unsubscription:\n" +
                  errors
                    .map(function (err, i) {
                      return i + 1 + ") " + err.toString();
                    })
                    .join("\n  ")
                : "";
              this.name = "UnsubscriptionError";
              this.errors = errors;
            };
          }
        );
        //# sourceMappingURL=UnsubscriptionError.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js":
      /*!****************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js ***!
  \****************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ arrRemove: () => /* binding */ arrRemove,
          /* harmony export */
        });
        function arrRemove(arr, item) {
          if (arr) {
            var index = arr.indexOf(item);
            0 <= index && arr.splice(index, 1);
          }
        }
        //# sourceMappingURL=arrRemove.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js":
      /*!***********************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js ***!
  \***********************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ createErrorClass: () =>
            /* binding */ createErrorClass,
          /* harmony export */
        });
        function createErrorClass(createImpl) {
          var _super = function (instance) {
            Error.call(instance);
            instance.stack = new Error().stack;
          };
          var ctorFunc = createImpl(_super);
          ctorFunc.prototype = Object.create(Error.prototype);
          ctorFunc.prototype.constructor = ctorFunc;
          return ctorFunc;
        }
        //# sourceMappingURL=createErrorClass.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js":
      /*!*******************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/errorContext.js ***!
  \*******************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ captureError: () => /* binding */ captureError,
          /* harmony export */ errorContext: () => /* binding */ errorContext,
          /* harmony export */
        });
        /* harmony import */ var _config__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ../config */ "./node_modules/rxjs/dist/esm5/internal/config.js"
          );

        var context = null;
        function errorContext(cb) {
          if (
            _config__WEBPACK_IMPORTED_MODULE_0__.config
              .useDeprecatedSynchronousErrorHandling
          ) {
            var isRoot = !context;
            if (isRoot) {
              context = { errorThrown: false, error: null };
            }
            cb();
            if (isRoot) {
              var _a = context,
                errorThrown = _a.errorThrown,
                error = _a.error;
              context = null;
              if (errorThrown) {
                throw error;
              }
            }
          } else {
            cb();
          }
        }
        function captureError(err) {
          if (
            _config__WEBPACK_IMPORTED_MODULE_0__.config
              .useDeprecatedSynchronousErrorHandling &&
            context
          ) {
            context.errorThrown = true;
            context.error = err;
          }
        }
        //# sourceMappingURL=errorContext.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/identity.js":
      /*!***************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/identity.js ***!
  \***************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ identity: () => /* binding */ identity,
          /* harmony export */
        });
        function identity(x) {
          return x;
        }
        //# sourceMappingURL=identity.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js":
      /*!*****************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/isFunction.js ***!
  \*****************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ isFunction: () => /* binding */ isFunction,
          /* harmony export */
        });
        function isFunction(value) {
          return typeof value === "function";
        }
        //# sourceMappingURL=isFunction.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/lift.js":
      /*!***********************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/lift.js ***!
  \***********************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ hasLift: () => /* binding */ hasLift,
          /* harmony export */ operate: () => /* binding */ operate,
          /* harmony export */
        });
        /* harmony import */ var _isFunction__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./isFunction */ "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );

        function hasLift(source) {
          return (0, _isFunction__WEBPACK_IMPORTED_MODULE_0__.isFunction)(
            source === null || source === void 0 ? void 0 : source.lift
          );
        }
        function operate(init) {
          return function (source) {
            if (hasLift(source)) {
              return source.lift(function (liftedSource) {
                try {
                  return init(liftedSource, this);
                } catch (err) {
                  this.error(err);
                }
              });
            }
            throw new TypeError("Unable to lift unknown Observable type");
          };
        }
        //# sourceMappingURL=lift.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/noop.js":
      /*!***********************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/noop.js ***!
  \***********************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ noop: () => /* binding */ noop,
          /* harmony export */
        });
        function noop() {}
        //# sourceMappingURL=noop.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/pipe.js":
      /*!***********************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/pipe.js ***!
  \***********************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ pipe: () => /* binding */ pipe,
          /* harmony export */ pipeFromArray: () => /* binding */ pipeFromArray,
          /* harmony export */
        });
        /* harmony import */ var _identity__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./identity */ "./node_modules/rxjs/dist/esm5/internal/util/identity.js"
          );

        function pipe() {
          var fns = [];
          for (var _i = 0; _i < arguments.length; _i++) {
            fns[_i] = arguments[_i];
          }
          return pipeFromArray(fns);
        }
        function pipeFromArray(fns) {
          if (fns.length === 0) {
            return _identity__WEBPACK_IMPORTED_MODULE_0__.identity;
          }
          if (fns.length === 1) {
            return fns[0];
          }
          return function piped(input) {
            return fns.reduce(function (prev, fn) {
              return fn(prev);
            }, input);
          };
        }
        //# sourceMappingURL=pipe.js.map

        /***/
      },

    /***/ "./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js":
      /*!***************************************************************************!*\
  !*** ./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js ***!
  \***************************************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ reportUnhandledError: () =>
            /* binding */ reportUnhandledError,
          /* harmony export */
        });
        /* harmony import */ var _config__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ../config */ "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
        /* harmony import */ var _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ../scheduler/timeoutProvider */ "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js"
          );

        function reportUnhandledError(err) {
          _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_0__.timeoutProvider.setTimeout(
            function () {
              var onUnhandledError =
                _config__WEBPACK_IMPORTED_MODULE_1__.config.onUnhandledError;
              if (onUnhandledError) {
                onUnhandledError(err);
              } else {
                throw err;
              }
            }
          );
        }
        //# sourceMappingURL=reportUnhandledError.js.map

        /***/
      },

    /***/ "./src/components/app-component.ts":
      /*!*****************************************!*\
  !*** ./src/components/app-component.ts ***!
  \*****************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony import */ var lit_html__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! lit-html */ "./node_modules/lit-html/development/lit-html.js"
          );
        /* harmony import */ var _user__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(/*! ./user */ "./src/components/user/index.ts");

        const template = lit_html__WEBPACK_IMPORTED_MODULE_0__.html`
    <user-table></user-table>
    <user-component></user-component>
`;
        class AppComponent extends HTMLElement {
          constructor() {
            super();
            this.attachShadow({ mode: "open" });
          }
          connectedCallback() {
            console.log("app component connected");
            this.render();
          }
          render() {
            (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(
              template,
              this.shadowRoot
            );
            const userTableComponent =
              this.shadowRoot.querySelector("user-table");
            const userComponent =
              this.shadowRoot.querySelector("user-component");
            userTableComponent.addEventListener(
              _user__WEBPACK_IMPORTED_MODULE_1__.USER_SELECTED_EVENT,
              (e) => {
                const user = e.detail.user;
                userComponent.setAttribute("selected-user", user.id);
                userComponent.style.display = "block";
                userTableComponent.style.display = "none";
                console.log("event: user selected:", user);
              }
            );
          }
        }
        customElements.define("app-component", AppComponent);

        /***/
      },

    /***/ "./src/components/user/index.ts":
      /*!**************************************!*\
  !*** ./src/components/user/index.ts ***!
  \**************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ USER_SELECTED_EVENT: () =>
            /* binding */ USER_SELECTED_EVENT,
          /* harmony export */
        });
        /* harmony import */ var _user_component__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! ./user-component */ "./src/components/user/user-component.ts"
          );
        /* harmony import */ var _user_table_component__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! ./user-table-component */ "./src/components/user/user-table-component.ts"
          );

        const USER_SELECTED_EVENT = "user-selected";

        /***/
      },

    /***/ "./src/components/user/user-component.ts":
      /*!***********************************************!*\
  !*** ./src/components/user/user-component.ts ***!
  \***********************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony import */ var lit_html__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! lit-html */ "./node_modules/lit-html/development/lit-html.js"
          );

        const template = lit_html__WEBPACK_IMPORTED_MODULE_0__.html`
    <div>TODO: implement this</div>
`;
        class UserComponent extends HTMLElement {
          constructor() {
            super();
            this.attachShadow({ mode: "open" });
          }
          connectedCallback() {
            this.render();
          }
          render() {
            (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(
              template,
              this.shadowRoot
            );
          }
        }
        customElements.define("user-component", UserComponent);

        /***/
      },

    /***/ "./src/components/user/user-table-component.ts":
      /*!*****************************************************!*\
  !*** ./src/components/user/user-table-component.ts ***!
  \*****************************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ UserTableComponent: () =>
            /* binding */ UserTableComponent,
          /* harmony export */
        });
        /* harmony import */ var lit_html__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! lit-html */ "./node_modules/lit-html/development/lit-html.js"
          );
        /* harmony import */ var ___WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(/*! . */ "./src/components/user/index.ts");
        /* harmony import */ var _user_service__WEBPACK_IMPORTED_MODULE_2__ =
          __webpack_require__(
            /*! ../../user-service */ "./src/user-service.ts"
          );
        /* harmony import */ var _model_store__WEBPACK_IMPORTED_MODULE_3__ =
          __webpack_require__(/*! ../../model/store */ "./src/model/store.ts");
        /* harmony import */ var rxjs__WEBPACK_IMPORTED_MODULE_4__ =
          __webpack_require__(
            /*! rxjs */ "./node_modules/rxjs/dist/esm5/internal/operators/map.js"
          );
        /* harmony import */ var rxjs__WEBPACK_IMPORTED_MODULE_5__ =
          __webpack_require__(
            /*! rxjs */ "./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js"
          );

        const rowTemplate = (
          user,
          onclick
        ) => lit_html__WEBPACK_IMPORTED_MODULE_0__.html`
    <tr @click=${() => onclick(user)}>
        <td >${user.id}</td><td>${user.name}</td>
    </tr>
`;
        class UserTableComponent extends HTMLElement {
          constructor() {
            super();
            this._updateComplete = false;
            this.attachShadow({ mode: "open" });
          }
          get updateComplete() {
            return this._updateComplete;
          }
          connectedCallback() {
            console.log("connected usertable");
            _user_service__WEBPACK_IMPORTED_MODULE_2__["default"].fetchAll();
            _model_store__WEBPACK_IMPORTED_MODULE_3__["default"]
              .pipe(
                (0, rxjs__WEBPACK_IMPORTED_MODULE_4__.map)(
                  (model) => model.users
                ),
                (0, rxjs__WEBPACK_IMPORTED_MODULE_5__.distinctUntilChanged)()
              )
              .subscribe((users) => {
                this.render(users);
              });
          }
          render(users) {
            this._updateComplete = false;
            const userClicked = (user) => {
              alert(`user ${user.name} selected`);
              this.dispatchEvent(
                new CustomEvent(
                  ___WEBPACK_IMPORTED_MODULE_1__.USER_SELECTED_EVENT,
                  { detail: { user } }
                )
              );
            };
            const rows = users.map((user) => rowTemplate(user, userClicked));
            const tableTemplate = lit_html__WEBPACK_IMPORTED_MODULE_0__.html`
            <link rel="stylesheet" href="https://www.w3schools.com/w3css/4/w3.css">
            <table class="w3-table-all">
                <thead>
                    <tr>
                        <th>Id</th><th>Name</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows}
                </tbody>
            </table>
        `;
            (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(
              tableTemplate,
              this.shadowRoot
            );
            this._updateComplete = true;
          }
        }
        customElements.define("user-table", UserTableComponent);

        /***/
      },

    /***/ "./src/model/store.ts":
      /*!****************************!*\
  !*** ./src/model/store.ts ***!
  \****************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ default: () => __WEBPACK_DEFAULT_EXPORT__,
          /* harmony export */ setUsers: () => /* binding */ setUsers,
          /* harmony export */
        });
        /* harmony import */ var immer__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(
            /*! immer */ "./node_modules/immer/dist/immer.esm.mjs"
          );
        /* harmony import */ var rxjs__WEBPACK_IMPORTED_MODULE_1__ =
          __webpack_require__(
            /*! rxjs */ "./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js"
          );

        const store = createStore();
        function setUsers(users) {
          let nextState = (0, immer__WEBPACK_IMPORTED_MODULE_0__["default"])(
            store.getValue(),
            (draft) => {
              draft.users = users;
            }
          );
          store.next(nextState);
        }
        function createStore() {
          const initialState = {
            users: [],
          };
          return new rxjs__WEBPACK_IMPORTED_MODULE_1__.BehaviorSubject(
            initialState
          );
        }
        /* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = store;

        /***/
      },

    /***/ "./src/user-service.ts":
      /*!*****************************!*\
  !*** ./src/user-service.ts ***!
  \*****************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ default: () => __WEBPACK_DEFAULT_EXPORT__,
          /* harmony export */
        });
        /* harmony import */ var _model_store__WEBPACK_IMPORTED_MODULE_0__ =
          __webpack_require__(/*! ./model/store */ "./src/model/store.ts");

        const USER_URL = "./g.tikautz/api/api/user";
        class UserService {
          async fetchAll() {
            const response = await fetch(USER_URL);
            const users = await response.json();
            (0, _model_store__WEBPACK_IMPORTED_MODULE_0__.setUsers)(users);
          }
        }
        const userService = new UserService();
        /* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ =
          userService;

        /***/
      },

    /***/ "./node_modules/tslib/tslib.es6.js":
      /*!*****************************************!*\
  !*** ./node_modules/tslib/tslib.es6.js ***!
  \*****************************************/
      /***/ (
        __unused_webpack_module,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ __assign: () => /* binding */ __assign,
          /* harmony export */ __asyncDelegator: () =>
            /* binding */ __asyncDelegator,
          /* harmony export */ __asyncGenerator: () =>
            /* binding */ __asyncGenerator,
          /* harmony export */ __asyncValues: () => /* binding */ __asyncValues,
          /* harmony export */ __await: () => /* binding */ __await,
          /* harmony export */ __awaiter: () => /* binding */ __awaiter,
          /* harmony export */ __classPrivateFieldGet: () =>
            /* binding */ __classPrivateFieldGet,
          /* harmony export */ __classPrivateFieldIn: () =>
            /* binding */ __classPrivateFieldIn,
          /* harmony export */ __classPrivateFieldSet: () =>
            /* binding */ __classPrivateFieldSet,
          /* harmony export */ __createBinding: () =>
            /* binding */ __createBinding,
          /* harmony export */ __decorate: () => /* binding */ __decorate,
          /* harmony export */ __esDecorate: () => /* binding */ __esDecorate,
          /* harmony export */ __exportStar: () => /* binding */ __exportStar,
          /* harmony export */ __extends: () => /* binding */ __extends,
          /* harmony export */ __generator: () => /* binding */ __generator,
          /* harmony export */ __importDefault: () =>
            /* binding */ __importDefault,
          /* harmony export */ __importStar: () => /* binding */ __importStar,
          /* harmony export */ __makeTemplateObject: () =>
            /* binding */ __makeTemplateObject,
          /* harmony export */ __metadata: () => /* binding */ __metadata,
          /* harmony export */ __param: () => /* binding */ __param,
          /* harmony export */ __propKey: () => /* binding */ __propKey,
          /* harmony export */ __read: () => /* binding */ __read,
          /* harmony export */ __rest: () => /* binding */ __rest,
          /* harmony export */ __runInitializers: () =>
            /* binding */ __runInitializers,
          /* harmony export */ __setFunctionName: () =>
            /* binding */ __setFunctionName,
          /* harmony export */ __spread: () => /* binding */ __spread,
          /* harmony export */ __spreadArray: () => /* binding */ __spreadArray,
          /* harmony export */ __spreadArrays: () =>
            /* binding */ __spreadArrays,
          /* harmony export */ __values: () => /* binding */ __values,
          /* harmony export */
        });
        /******************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */
        /* global Reflect, Promise */

        var extendStatics = function (d, b) {
          extendStatics =
            Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array &&
              function (d, b) {
                d.__proto__ = b;
              }) ||
            function (d, b) {
              for (var p in b)
                if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p];
            };
          return extendStatics(d, b);
        };

        function __extends(d, b) {
          if (typeof b !== "function" && b !== null)
            throw new TypeError(
              "Class extends value " +
                String(b) +
                " is not a constructor or null"
            );
          extendStatics(d, b);
          function __() {
            this.constructor = d;
          }
          d.prototype =
            b === null
              ? Object.create(b)
              : ((__.prototype = b.prototype), new __());
        }

        var __assign = function () {
          __assign =
            Object.assign ||
            function __assign(t) {
              for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s)
                  if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];
              }
              return t;
            };
          return __assign.apply(this, arguments);
        };

        function __rest(s, e) {
          var t = {};
          for (var p in s)
            if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
              t[p] = s[p];
          if (s != null && typeof Object.getOwnPropertySymbols === "function")
            for (
              var i = 0, p = Object.getOwnPropertySymbols(s);
              i < p.length;
              i++
            ) {
              if (
                e.indexOf(p[i]) < 0 &&
                Object.prototype.propertyIsEnumerable.call(s, p[i])
              )
                t[p[i]] = s[p[i]];
            }
          return t;
        }

        function __decorate(decorators, target, key, desc) {
          var c = arguments.length,
            r =
              c < 3
                ? target
                : desc === null
                ? (desc = Object.getOwnPropertyDescriptor(target, key))
                : desc,
            d;
          if (
            typeof Reflect === "object" &&
            typeof Reflect.decorate === "function"
          )
            r = Reflect.decorate(decorators, target, key, desc);
          else
            for (var i = decorators.length - 1; i >= 0; i--)
              if ((d = decorators[i]))
                r =
                  (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) ||
                  r;
          return c > 3 && r && Object.defineProperty(target, key, r), r;
        }

        function __param(paramIndex, decorator) {
          return function (target, key) {
            decorator(target, key, paramIndex);
          };
        }

        function __esDecorate(
          ctor,
          descriptorIn,
          decorators,
          contextIn,
          initializers,
          extraInitializers
        ) {
          function accept(f) {
            if (f !== void 0 && typeof f !== "function")
              throw new TypeError("Function expected");
            return f;
          }
          var kind = contextIn.kind,
            key =
              kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
          var target =
            !descriptorIn && ctor
              ? contextIn["static"]
                ? ctor
                : ctor.prototype
              : null;
          var descriptor =
            descriptorIn ||
            (target
              ? Object.getOwnPropertyDescriptor(target, contextIn.name)
              : {});
          var _,
            done = false;
          for (var i = decorators.length - 1; i >= 0; i--) {
            var context = {};
            for (var p in contextIn)
              context[p] = p === "access" ? {} : contextIn[p];
            for (var p in contextIn.access)
              context.access[p] = contextIn.access[p];
            context.addInitializer = function (f) {
              if (done)
                throw new TypeError(
                  "Cannot add initializers after decoration has completed"
                );
              extraInitializers.push(accept(f || null));
            };
            var result = (0, decorators[i])(
              kind === "accessor"
                ? { get: descriptor.get, set: descriptor.set }
                : descriptor[key],
              context
            );
            if (kind === "accessor") {
              if (result === void 0) continue;
              if (result === null || typeof result !== "object")
                throw new TypeError("Object expected");
              if ((_ = accept(result.get))) descriptor.get = _;
              if ((_ = accept(result.set))) descriptor.set = _;
              if ((_ = accept(result.init))) initializers.push(_);
            } else if ((_ = accept(result))) {
              if (kind === "field") initializers.push(_);
              else descriptor[key] = _;
            }
          }
          if (target) Object.defineProperty(target, contextIn.name, descriptor);
          done = true;
        }

        function __runInitializers(thisArg, initializers, value) {
          var useValue = arguments.length > 2;
          for (var i = 0; i < initializers.length; i++) {
            value = useValue
              ? initializers[i].call(thisArg, value)
              : initializers[i].call(thisArg);
          }
          return useValue ? value : void 0;
        }

        function __propKey(x) {
          return typeof x === "symbol" ? x : "".concat(x);
        }

        function __setFunctionName(f, name, prefix) {
          if (typeof name === "symbol")
            name = name.description ? "[".concat(name.description, "]") : "";
          return Object.defineProperty(f, "name", {
            configurable: true,
            value: prefix ? "".concat(prefix, " ", name) : name,
          });
        }

        function __metadata(metadataKey, metadataValue) {
          if (
            typeof Reflect === "object" &&
            typeof Reflect.metadata === "function"
          )
            return Reflect.metadata(metadataKey, metadataValue);
        }

        function __awaiter(thisArg, _arguments, P, generator) {
          function adopt(value) {
            return value instanceof P
              ? value
              : new P(function (resolve) {
                  resolve(value);
                });
          }
          return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) {
              try {
                step(generator.next(value));
              } catch (e) {
                reject(e);
              }
            }
            function rejected(value) {
              try {
                step(generator["throw"](value));
              } catch (e) {
                reject(e);
              }
            }
            function step(result) {
              result.done
                ? resolve(result.value)
                : adopt(result.value).then(fulfilled, rejected);
            }
            step(
              (generator = generator.apply(thisArg, _arguments || [])).next()
            );
          });
        }

        function __generator(thisArg, body) {
          var _ = {
              label: 0,
              sent: function () {
                if (t[0] & 1) throw t[1];
                return t[1];
              },
              trys: [],
              ops: [],
            },
            f,
            y,
            t,
            g;
          return (
            (g = { next: verb(0), throw: verb(1), return: verb(2) }),
            typeof Symbol === "function" &&
              (g[Symbol.iterator] = function () {
                return this;
              }),
            g
          );
          function verb(n) {
            return function (v) {
              return step([n, v]);
            };
          }
          function step(op) {
            if (f) throw new TypeError("Generator is already executing.");
            while ((g && ((g = 0), op[0] && (_ = 0)), _))
              try {
                if (
                  ((f = 1),
                  y &&
                    (t =
                      op[0] & 2
                        ? y["return"]
                        : op[0]
                        ? y["throw"] || ((t = y["return"]) && t.call(y), 0)
                        : y.next) &&
                    !(t = t.call(y, op[1])).done)
                )
                  return t;
                if (((y = 0), t)) op = [op[0] & 2, t.value];
                switch (op[0]) {
                  case 0:
                  case 1:
                    t = op;
                    break;
                  case 4:
                    _.label++;
                    return { value: op[1], done: false };
                  case 5:
                    _.label++;
                    y = op[1];
                    op = [0];
                    continue;
                  case 7:
                    op = _.ops.pop();
                    _.trys.pop();
                    continue;
                  default:
                    if (
                      !((t = _.trys), (t = t.length > 0 && t[t.length - 1])) &&
                      (op[0] === 6 || op[0] === 2)
                    ) {
                      _ = 0;
                      continue;
                    }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) {
                      _.label = op[1];
                      break;
                    }
                    if (op[0] === 6 && _.label < t[1]) {
                      _.label = t[1];
                      t = op;
                      break;
                    }
                    if (t && _.label < t[2]) {
                      _.label = t[2];
                      _.ops.push(op);
                      break;
                    }
                    if (t[2]) _.ops.pop();
                    _.trys.pop();
                    continue;
                }
                op = body.call(thisArg, _);
              } catch (e) {
                op = [6, e];
                y = 0;
              } finally {
                f = t = 0;
              }
            if (op[0] & 5) throw op[1];
            return { value: op[0] ? op[1] : void 0, done: true };
          }
        }

        var __createBinding = Object.create
          ? function (o, m, k, k2) {
              if (k2 === undefined) k2 = k;
              var desc = Object.getOwnPropertyDescriptor(m, k);
              if (
                !desc ||
                ("get" in desc
                  ? !m.__esModule
                  : desc.writable || desc.configurable)
              ) {
                desc = {
                  enumerable: true,
                  get: function () {
                    return m[k];
                  },
                };
              }
              Object.defineProperty(o, k2, desc);
            }
          : function (o, m, k, k2) {
              if (k2 === undefined) k2 = k;
              o[k2] = m[k];
            };

        function __exportStar(m, o) {
          for (var p in m)
            if (p !== "default" && !Object.prototype.hasOwnProperty.call(o, p))
              __createBinding(o, m, p);
        }

        function __values(o) {
          var s = typeof Symbol === "function" && Symbol.iterator,
            m = s && o[s],
            i = 0;
          if (m) return m.call(o);
          if (o && typeof o.length === "number")
            return {
              next: function () {
                if (o && i >= o.length) o = void 0;
                return { value: o && o[i++], done: !o };
              },
            };
          throw new TypeError(
            s ? "Object is not iterable." : "Symbol.iterator is not defined."
          );
        }

        function __read(o, n) {
          var m = typeof Symbol === "function" && o[Symbol.iterator];
          if (!m) return o;
          var i = m.call(o),
            r,
            ar = [],
            e;
          try {
            while ((n === void 0 || n-- > 0) && !(r = i.next()).done)
              ar.push(r.value);
          } catch (error) {
            e = { error: error };
          } finally {
            try {
              if (r && !r.done && (m = i["return"])) m.call(i);
            } finally {
              if (e) throw e.error;
            }
          }
          return ar;
        }

        /** @deprecated */
        function __spread() {
          for (var ar = [], i = 0; i < arguments.length; i++)
            ar = ar.concat(__read(arguments[i]));
          return ar;
        }

        /** @deprecated */
        function __spreadArrays() {
          for (var s = 0, i = 0, il = arguments.length; i < il; i++)
            s += arguments[i].length;
          for (var r = Array(s), k = 0, i = 0; i < il; i++)
            for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
              r[k] = a[j];
          return r;
        }

        function __spreadArray(to, from, pack) {
          if (pack || arguments.length === 2)
            for (var i = 0, l = from.length, ar; i < l; i++) {
              if (ar || !(i in from)) {
                if (!ar) ar = Array.prototype.slice.call(from, 0, i);
                ar[i] = from[i];
              }
            }
          return to.concat(ar || Array.prototype.slice.call(from));
        }

        function __await(v) {
          return this instanceof __await
            ? ((this.v = v), this)
            : new __await(v);
        }

        function __asyncGenerator(thisArg, _arguments, generator) {
          if (!Symbol.asyncIterator)
            throw new TypeError("Symbol.asyncIterator is not defined.");
          var g = generator.apply(thisArg, _arguments || []),
            i,
            q = [];
          return (
            (i = {}),
            verb("next"),
            verb("throw"),
            verb("return"),
            (i[Symbol.asyncIterator] = function () {
              return this;
            }),
            i
          );
          function verb(n) {
            if (g[n])
              i[n] = function (v) {
                return new Promise(function (a, b) {
                  q.push([n, v, a, b]) > 1 || resume(n, v);
                });
              };
          }
          function resume(n, v) {
            try {
              step(g[n](v));
            } catch (e) {
              settle(q[0][3], e);
            }
          }
          function step(r) {
            r.value instanceof __await
              ? Promise.resolve(r.value.v).then(fulfill, reject)
              : settle(q[0][2], r);
          }
          function fulfill(value) {
            resume("next", value);
          }
          function reject(value) {
            resume("throw", value);
          }
          function settle(f, v) {
            if ((f(v), q.shift(), q.length)) resume(q[0][0], q[0][1]);
          }
        }

        function __asyncDelegator(o) {
          var i, p;
          return (
            (i = {}),
            verb("next"),
            verb("throw", function (e) {
              throw e;
            }),
            verb("return"),
            (i[Symbol.iterator] = function () {
              return this;
            }),
            i
          );
          function verb(n, f) {
            i[n] = o[n]
              ? function (v) {
                  return (p = !p)
                    ? { value: __await(o[n](v)), done: false }
                    : f
                    ? f(v)
                    : v;
                }
              : f;
          }
        }

        function __asyncValues(o) {
          if (!Symbol.asyncIterator)
            throw new TypeError("Symbol.asyncIterator is not defined.");
          var m = o[Symbol.asyncIterator],
            i;
          return m
            ? m.call(o)
            : ((o =
                typeof __values === "function"
                  ? __values(o)
                  : o[Symbol.iterator]()),
              (i = {}),
              verb("next"),
              verb("throw"),
              verb("return"),
              (i[Symbol.asyncIterator] = function () {
                return this;
              }),
              i);
          function verb(n) {
            i[n] =
              o[n] &&
              function (v) {
                return new Promise(function (resolve, reject) {
                  (v = o[n](v)), settle(resolve, reject, v.done, v.value);
                });
              };
          }
          function settle(resolve, reject, d, v) {
            Promise.resolve(v).then(function (v) {
              resolve({ value: v, done: d });
            }, reject);
          }
        }

        function __makeTemplateObject(cooked, raw) {
          if (Object.defineProperty) {
            Object.defineProperty(cooked, "raw", { value: raw });
          } else {
            cooked.raw = raw;
          }
          return cooked;
        }

        var __setModuleDefault = Object.create
          ? function (o, v) {
              Object.defineProperty(o, "default", {
                enumerable: true,
                value: v,
              });
            }
          : function (o, v) {
              o["default"] = v;
            };

        function __importStar(mod) {
          if (mod && mod.__esModule) return mod;
          var result = {};
          if (mod != null)
            for (var k in mod)
              if (
                k !== "default" &&
                Object.prototype.hasOwnProperty.call(mod, k)
              )
                __createBinding(result, mod, k);
          __setModuleDefault(result, mod);
          return result;
        }

        function __importDefault(mod) {
          return mod && mod.__esModule ? mod : { default: mod };
        }

        function __classPrivateFieldGet(receiver, state, kind, f) {
          if (kind === "a" && !f)
            throw new TypeError(
              "Private accessor was defined without a getter"
            );
          if (
            typeof state === "function"
              ? receiver !== state || !f
              : !state.has(receiver)
          )
            throw new TypeError(
              "Cannot read private member from an object whose class did not declare it"
            );
          return kind === "m"
            ? f
            : kind === "a"
            ? f.call(receiver)
            : f
            ? f.value
            : state.get(receiver);
        }

        function __classPrivateFieldSet(receiver, state, value, kind, f) {
          if (kind === "m")
            throw new TypeError("Private method is not writable");
          if (kind === "a" && !f)
            throw new TypeError(
              "Private accessor was defined without a setter"
            );
          if (
            typeof state === "function"
              ? receiver !== state || !f
              : !state.has(receiver)
          )
            throw new TypeError(
              "Cannot write private member to an object whose class did not declare it"
            );
          return (
            kind === "a"
              ? f.call(receiver, value)
              : f
              ? (f.value = value)
              : state.set(receiver, value),
            value
          );
        }

        function __classPrivateFieldIn(state, receiver) {
          if (
            receiver === null ||
            (typeof receiver !== "object" && typeof receiver !== "function")
          )
            throw new TypeError("Cannot use 'in' operator on non-object");
          return typeof state === "function"
            ? receiver === state
            : state.has(receiver);
        }

        /***/
      },

    /***/ "./node_modules/immer/dist/immer.esm.mjs":
      /*!***********************************************!*\
  !*** ./node_modules/immer/dist/immer.esm.mjs ***!
  \***********************************************/
      /***/ (
        __unused_webpack___webpack_module__,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ Immer: () => /* binding */ un,
          /* harmony export */ applyPatches: () => /* binding */ pn,
          /* harmony export */ castDraft: () => /* binding */ K,
          /* harmony export */ castImmutable: () => /* binding */ $,
          /* harmony export */ createDraft: () => /* binding */ ln,
          /* harmony export */ current: () => /* binding */ R,
          /* harmony export */ default: () => __WEBPACK_DEFAULT_EXPORT__,
          /* harmony export */ enableAllPlugins: () => /* binding */ J,
          /* harmony export */ enableES5: () => /* binding */ F,
          /* harmony export */ enableMapSet: () => /* binding */ C,
          /* harmony export */ enablePatches: () => /* binding */ T,
          /* harmony export */ finishDraft: () => /* binding */ dn,
          /* harmony export */ freeze: () => /* binding */ d,
          /* harmony export */ immerable: () => /* binding */ L,
          /* harmony export */ isDraft: () => /* binding */ r,
          /* harmony export */ isDraftable: () => /* binding */ t,
          /* harmony export */ nothing: () => /* binding */ H,
          /* harmony export */ original: () => /* binding */ e,
          /* harmony export */ produce: () => /* binding */ fn,
          /* harmony export */ produceWithPatches: () => /* binding */ cn,
          /* harmony export */ setAutoFreeze: () => /* binding */ sn,
          /* harmony export */ setUseProxies: () => /* binding */ vn,
          /* harmony export */
        });
        function n(n) {
          for (
            var r = arguments.length, t = Array(r > 1 ? r - 1 : 0), e = 1;
            e < r;
            e++
          )
            t[e - 1] = arguments[e];
          if (true) {
            var i = Y[n],
              o = i
                ? "function" == typeof i
                  ? i.apply(null, t)
                  : i
                : "unknown error nr: " + n;
            throw Error("[Immer] " + o);
          }
          throw Error(
            "[Immer] minified error nr: " +
              n +
              (t.length
                ? " " +
                  t
                    .map(function (n) {
                      return "'" + n + "'";
                    })
                    .join(",")
                : "") +
              ". Find the full error at: https://bit.ly/3cXEKWf"
          );
        }
        function r(n) {
          return !!n && !!n[Q];
        }
        function t(n) {
          var r;
          return (
            !!n &&
            ((function (n) {
              if (!n || "object" != typeof n) return !1;
              var r = Object.getPrototypeOf(n);
              if (null === r) return !0;
              var t =
                Object.hasOwnProperty.call(r, "constructor") && r.constructor;
              return (
                t === Object ||
                ("function" == typeof t && Function.toString.call(t) === Z)
              );
            })(n) ||
              Array.isArray(n) ||
              !!n[L] ||
              !!(null === (r = n.constructor) || void 0 === r
                ? void 0
                : r[L]) ||
              s(n) ||
              v(n))
          );
        }
        function e(t) {
          return r(t) || n(23, t), t[Q].t;
        }
        function i(n, r, t) {
          void 0 === t && (t = !1),
            0 === o(n)
              ? (t ? Object.keys : nn)(n).forEach(function (e) {
                  (t && "symbol" == typeof e) || r(e, n[e], n);
                })
              : n.forEach(function (t, e) {
                  return r(e, t, n);
                });
        }
        function o(n) {
          var r = n[Q];
          return r
            ? r.i > 3
              ? r.i - 4
              : r.i
            : Array.isArray(n)
            ? 1
            : s(n)
            ? 2
            : v(n)
            ? 3
            : 0;
        }
        function u(n, r) {
          return 2 === o(n)
            ? n.has(r)
            : Object.prototype.hasOwnProperty.call(n, r);
        }
        function a(n, r) {
          return 2 === o(n) ? n.get(r) : n[r];
        }
        function f(n, r, t) {
          var e = o(n);
          2 === e ? n.set(r, t) : 3 === e ? n.add(t) : (n[r] = t);
        }
        function c(n, r) {
          return n === r ? 0 !== n || 1 / n == 1 / r : n != n && r != r;
        }
        function s(n) {
          return X && n instanceof Map;
        }
        function v(n) {
          return q && n instanceof Set;
        }
        function p(n) {
          return n.o || n.t;
        }
        function l(n) {
          if (Array.isArray(n)) return Array.prototype.slice.call(n);
          var r = rn(n);
          delete r[Q];
          for (var t = nn(r), e = 0; e < t.length; e++) {
            var i = t[e],
              o = r[i];
            !1 === o.writable && ((o.writable = !0), (o.configurable = !0)),
              (o.get || o.set) &&
                (r[i] = {
                  configurable: !0,
                  writable: !0,
                  enumerable: o.enumerable,
                  value: n[i],
                });
          }
          return Object.create(Object.getPrototypeOf(n), r);
        }
        function d(n, e) {
          return (
            void 0 === e && (e = !1),
            y(n) ||
              r(n) ||
              !t(n) ||
              (o(n) > 1 && (n.set = n.add = n.clear = n.delete = h),
              Object.freeze(n),
              e &&
                i(
                  n,
                  function (n, r) {
                    return d(r, !0);
                  },
                  !0
                )),
            n
          );
        }
        function h() {
          n(2);
        }
        function y(n) {
          return null == n || "object" != typeof n || Object.isFrozen(n);
        }
        function b(r) {
          var t = tn[r];
          return t || n(18, r), t;
        }
        function m(n, r) {
          tn[n] || (tn[n] = r);
        }
        function _() {
          return false || U || n(0), U;
        }
        function j(n, r) {
          r && (b("Patches"), (n.u = []), (n.s = []), (n.v = r));
        }
        function O(n) {
          g(n), n.p.forEach(S), (n.p = null);
        }
        function g(n) {
          n === U && (U = n.l);
        }
        function w(n) {
          return (U = { p: [], l: U, h: n, m: !0, _: 0 });
        }
        function S(n) {
          var r = n[Q];
          0 === r.i || 1 === r.i ? r.j() : (r.O = !0);
        }
        function P(r, e) {
          e._ = e.p.length;
          var i = e.p[0],
            o = void 0 !== r && r !== i;
          return (
            e.h.g || b("ES5").S(e, r, o),
            o
              ? (i[Q].P && (O(e), n(4)),
                t(r) && ((r = M(e, r)), e.l || x(e, r)),
                e.u && b("Patches").M(i[Q].t, r, e.u, e.s))
              : (r = M(e, i, [])),
            O(e),
            e.u && e.v(e.u, e.s),
            r !== H ? r : void 0
          );
        }
        function M(n, r, t) {
          if (y(r)) return r;
          var e = r[Q];
          if (!e)
            return (
              i(
                r,
                function (i, o) {
                  return A(n, e, r, i, o, t);
                },
                !0
              ),
              r
            );
          if (e.A !== n) return r;
          if (!e.P) return x(n, e.t, !0), e.t;
          if (!e.I) {
            (e.I = !0), e.A._--;
            var o = 4 === e.i || 5 === e.i ? (e.o = l(e.k)) : e.o,
              u = o,
              a = !1;
            3 === e.i && ((u = new Set(o)), o.clear(), (a = !0)),
              i(u, function (r, i) {
                return A(n, e, o, r, i, t, a);
              }),
              x(n, o, !1),
              t && n.u && b("Patches").N(e, t, n.u, n.s);
          }
          return e.o;
        }
        function A(e, i, o, a, c, s, v) {
          if ((true && c === o && n(5), r(c))) {
            var p = M(
              e,
              c,
              s && i && 3 !== i.i && !u(i.R, a) ? s.concat(a) : void 0
            );
            if ((f(o, a, p), !r(p))) return;
            e.m = !1;
          } else v && o.add(c);
          if (t(c) && !y(c)) {
            if (!e.h.D && e._ < 1) return;
            M(e, c), (i && i.A.l) || x(e, c);
          }
        }
        function x(n, r, t) {
          void 0 === t && (t = !1), !n.l && n.h.D && n.m && d(r, t);
        }
        function z(n, r) {
          var t = n[Q];
          return (t ? p(t) : n)[r];
        }
        function I(n, r) {
          if (r in n)
            for (var t = Object.getPrototypeOf(n); t; ) {
              var e = Object.getOwnPropertyDescriptor(t, r);
              if (e) return e;
              t = Object.getPrototypeOf(t);
            }
        }
        function k(n) {
          n.P || ((n.P = !0), n.l && k(n.l));
        }
        function E(n) {
          n.o || (n.o = l(n.t));
        }
        function N(n, r, t) {
          var e = s(r)
            ? b("MapSet").F(r, t)
            : v(r)
            ? b("MapSet").T(r, t)
            : n.g
            ? (function (n, r) {
                var t = Array.isArray(n),
                  e = {
                    i: t ? 1 : 0,
                    A: r ? r.A : _(),
                    P: !1,
                    I: !1,
                    R: {},
                    l: r,
                    t: n,
                    k: null,
                    o: null,
                    j: null,
                    C: !1,
                  },
                  i = e,
                  o = en;
                t && ((i = [e]), (o = on));
                var u = Proxy.revocable(i, o),
                  a = u.revoke,
                  f = u.proxy;
                return (e.k = f), (e.j = a), f;
              })(r, t)
            : b("ES5").J(r, t);
          return (t ? t.A : _()).p.push(e), e;
        }
        function R(e) {
          return (
            r(e) || n(22, e),
            (function n(r) {
              if (!t(r)) return r;
              var e,
                u = r[Q],
                c = o(r);
              if (u) {
                if (!u.P && (u.i < 4 || !b("ES5").K(u))) return u.t;
                (u.I = !0), (e = D(r, c)), (u.I = !1);
              } else e = D(r, c);
              return (
                i(e, function (r, t) {
                  (u && a(u.t, r) === t) || f(e, r, n(t));
                }),
                3 === c ? new Set(e) : e
              );
            })(e)
          );
        }
        function D(n, r) {
          switch (r) {
            case 2:
              return new Map(n);
            case 3:
              return Array.from(n);
          }
          return l(n);
        }
        function F() {
          function t(n, r) {
            var t = s[n];
            return (
              t
                ? (t.enumerable = r)
                : (s[n] = t =
                    {
                      configurable: !0,
                      enumerable: r,
                      get: function () {
                        var r = this[Q];
                        return true && f(r), en.get(r, n);
                      },
                      set: function (r) {
                        var t = this[Q];
                        true && f(t), en.set(t, n, r);
                      },
                    }),
              t
            );
          }
          function e(n) {
            for (var r = n.length - 1; r >= 0; r--) {
              var t = n[r][Q];
              if (!t.P)
                switch (t.i) {
                  case 5:
                    a(t) && k(t);
                    break;
                  case 4:
                    o(t) && k(t);
                }
            }
          }
          function o(n) {
            for (
              var r = n.t, t = n.k, e = nn(t), i = e.length - 1;
              i >= 0;
              i--
            ) {
              var o = e[i];
              if (o !== Q) {
                var a = r[o];
                if (void 0 === a && !u(r, o)) return !0;
                var f = t[o],
                  s = f && f[Q];
                if (s ? s.t !== a : !c(f, a)) return !0;
              }
            }
            var v = !!r[Q];
            return e.length !== nn(r).length + (v ? 0 : 1);
          }
          function a(n) {
            var r = n.k;
            if (r.length !== n.t.length) return !0;
            var t = Object.getOwnPropertyDescriptor(r, r.length - 1);
            if (t && !t.get) return !0;
            for (var e = 0; e < r.length; e++)
              if (!r.hasOwnProperty(e)) return !0;
            return !1;
          }
          function f(r) {
            r.O && n(3, JSON.stringify(p(r)));
          }
          var s = {};
          m("ES5", {
            J: function (n, r) {
              var e = Array.isArray(n),
                i = (function (n, r) {
                  if (n) {
                    for (var e = Array(r.length), i = 0; i < r.length; i++)
                      Object.defineProperty(e, "" + i, t(i, !0));
                    return e;
                  }
                  var o = rn(r);
                  delete o[Q];
                  for (var u = nn(o), a = 0; a < u.length; a++) {
                    var f = u[a];
                    o[f] = t(f, n || !!o[f].enumerable);
                  }
                  return Object.create(Object.getPrototypeOf(r), o);
                })(e, n),
                o = {
                  i: e ? 5 : 4,
                  A: r ? r.A : _(),
                  P: !1,
                  I: !1,
                  R: {},
                  l: r,
                  t: n,
                  k: i,
                  o: null,
                  O: !1,
                  C: !1,
                };
              return Object.defineProperty(i, Q, { value: o, writable: !0 }), i;
            },
            S: function (n, t, o) {
              o
                ? r(t) && t[Q].A === n && e(n.p)
                : (n.u &&
                    (function n(r) {
                      if (r && "object" == typeof r) {
                        var t = r[Q];
                        if (t) {
                          var e = t.t,
                            o = t.k,
                            f = t.R,
                            c = t.i;
                          if (4 === c)
                            i(o, function (r) {
                              r !== Q &&
                                (void 0 !== e[r] || u(e, r)
                                  ? f[r] || n(o[r])
                                  : ((f[r] = !0), k(t)));
                            }),
                              i(e, function (n) {
                                void 0 !== o[n] ||
                                  u(o, n) ||
                                  ((f[n] = !1), k(t));
                              });
                          else if (5 === c) {
                            if (
                              (a(t) && (k(t), (f.length = !0)),
                              o.length < e.length)
                            )
                              for (var s = o.length; s < e.length; s++)
                                f[s] = !1;
                            else
                              for (var v = e.length; v < o.length; v++)
                                f[v] = !0;
                            for (
                              var p = Math.min(o.length, e.length), l = 0;
                              l < p;
                              l++
                            )
                              o.hasOwnProperty(l) || (f[l] = !0),
                                void 0 === f[l] && n(o[l]);
                          }
                        }
                      }
                    })(n.p[0]),
                  e(n.p));
            },
            K: function (n) {
              return 4 === n.i ? o(n) : a(n);
            },
          });
        }
        function T() {
          function e(n) {
            if (!t(n)) return n;
            if (Array.isArray(n)) return n.map(e);
            if (s(n))
              return new Map(
                Array.from(n.entries()).map(function (n) {
                  return [n[0], e(n[1])];
                })
              );
            if (v(n)) return new Set(Array.from(n).map(e));
            var r = Object.create(Object.getPrototypeOf(n));
            for (var i in n) r[i] = e(n[i]);
            return u(n, L) && (r[L] = n[L]), r;
          }
          function f(n) {
            return r(n) ? e(n) : n;
          }
          var c = "add";
          m("Patches", {
            $: function (r, t) {
              return (
                t.forEach(function (t) {
                  for (
                    var i = t.path, u = t.op, f = r, s = 0;
                    s < i.length - 1;
                    s++
                  ) {
                    var v = o(f),
                      p = "" + i[s];
                    (0 !== v && 1 !== v) ||
                      ("__proto__" !== p && "constructor" !== p) ||
                      n(24),
                      "function" == typeof f && "prototype" === p && n(24),
                      "object" != typeof (f = a(f, p)) && n(15, i.join("/"));
                  }
                  var l = o(f),
                    d = e(t.value),
                    h = i[i.length - 1];
                  switch (u) {
                    case "replace":
                      switch (l) {
                        case 2:
                          return f.set(h, d);
                        case 3:
                          n(16);
                        default:
                          return (f[h] = d);
                      }
                    case c:
                      switch (l) {
                        case 1:
                          return "-" === h ? f.push(d) : f.splice(h, 0, d);
                        case 2:
                          return f.set(h, d);
                        case 3:
                          return f.add(d);
                        default:
                          return (f[h] = d);
                      }
                    case "remove":
                      switch (l) {
                        case 1:
                          return f.splice(h, 1);
                        case 2:
                          return f.delete(h);
                        case 3:
                          return f.delete(t.value);
                        default:
                          return delete f[h];
                      }
                    default:
                      n(17, u);
                  }
                }),
                r
              );
            },
            N: function (n, r, t, e) {
              switch (n.i) {
                case 0:
                case 4:
                case 2:
                  return (function (n, r, t, e) {
                    var o = n.t,
                      s = n.o;
                    i(n.R, function (n, i) {
                      var v = a(o, n),
                        p = a(s, n),
                        l = i ? (u(o, n) ? "replace" : c) : "remove";
                      if (v !== p || "replace" !== l) {
                        var d = r.concat(n);
                        t.push(
                          "remove" === l
                            ? { op: l, path: d }
                            : { op: l, path: d, value: p }
                        ),
                          e.push(
                            l === c
                              ? { op: "remove", path: d }
                              : "remove" === l
                              ? { op: c, path: d, value: f(v) }
                              : { op: "replace", path: d, value: f(v) }
                          );
                      }
                    });
                  })(n, r, t, e);
                case 5:
                case 1:
                  return (function (n, r, t, e) {
                    var i = n.t,
                      o = n.R,
                      u = n.o;
                    if (u.length < i.length) {
                      var a = [u, i];
                      (i = a[0]), (u = a[1]);
                      var s = [e, t];
                      (t = s[0]), (e = s[1]);
                    }
                    for (var v = 0; v < i.length; v++)
                      if (o[v] && u[v] !== i[v]) {
                        var p = r.concat([v]);
                        t.push({ op: "replace", path: p, value: f(u[v]) }),
                          e.push({ op: "replace", path: p, value: f(i[v]) });
                      }
                    for (var l = i.length; l < u.length; l++) {
                      var d = r.concat([l]);
                      t.push({ op: c, path: d, value: f(u[l]) });
                    }
                    i.length < u.length &&
                      e.push({
                        op: "replace",
                        path: r.concat(["length"]),
                        value: i.length,
                      });
                  })(n, r, t, e);
                case 3:
                  return (function (n, r, t, e) {
                    var i = n.t,
                      o = n.o,
                      u = 0;
                    i.forEach(function (n) {
                      if (!o.has(n)) {
                        var i = r.concat([u]);
                        t.push({ op: "remove", path: i, value: n }),
                          e.unshift({ op: c, path: i, value: n });
                      }
                      u++;
                    }),
                      (u = 0),
                      o.forEach(function (n) {
                        if (!i.has(n)) {
                          var o = r.concat([u]);
                          t.push({ op: c, path: o, value: n }),
                            e.unshift({ op: "remove", path: o, value: n });
                        }
                        u++;
                      });
                  })(n, r, t, e);
              }
            },
            M: function (n, r, t, e) {
              t.push({ op: "replace", path: [], value: r === H ? void 0 : r }),
                e.push({ op: "replace", path: [], value: n });
            },
          });
        }
        function C() {
          function r(n, r) {
            function t() {
              this.constructor = n;
            }
            a(n, r), (n.prototype = ((t.prototype = r.prototype), new t()));
          }
          function e(n) {
            n.o || ((n.R = new Map()), (n.o = new Map(n.t)));
          }
          function o(n) {
            n.o ||
              ((n.o = new Set()),
              n.t.forEach(function (r) {
                if (t(r)) {
                  var e = N(n.A.h, r, n);
                  n.p.set(r, e), n.o.add(e);
                } else n.o.add(r);
              }));
          }
          function u(r) {
            r.O && n(3, JSON.stringify(p(r)));
          }
          var a = function (n, r) {
              return (a =
                Object.setPrototypeOf ||
                ({ __proto__: [] } instanceof Array &&
                  function (n, r) {
                    n.__proto__ = r;
                  }) ||
                function (n, r) {
                  for (var t in r) r.hasOwnProperty(t) && (n[t] = r[t]);
                })(n, r);
            },
            f = (function () {
              function n(n, r) {
                return (
                  (this[Q] = {
                    i: 2,
                    l: r,
                    A: r ? r.A : _(),
                    P: !1,
                    I: !1,
                    o: void 0,
                    R: void 0,
                    t: n,
                    k: this,
                    C: !1,
                    O: !1,
                  }),
                  this
                );
              }
              r(n, Map);
              var o = n.prototype;
              return (
                Object.defineProperty(o, "size", {
                  get: function () {
                    return p(this[Q]).size;
                  },
                }),
                (o.has = function (n) {
                  return p(this[Q]).has(n);
                }),
                (o.set = function (n, r) {
                  var t = this[Q];
                  return (
                    u(t),
                    (p(t).has(n) && p(t).get(n) === r) ||
                      (e(t),
                      k(t),
                      t.R.set(n, !0),
                      t.o.set(n, r),
                      t.R.set(n, !0)),
                    this
                  );
                }),
                (o.delete = function (n) {
                  if (!this.has(n)) return !1;
                  var r = this[Q];
                  return (
                    u(r),
                    e(r),
                    k(r),
                    r.t.has(n) ? r.R.set(n, !1) : r.R.delete(n),
                    r.o.delete(n),
                    !0
                  );
                }),
                (o.clear = function () {
                  var n = this[Q];
                  u(n),
                    p(n).size &&
                      (e(n),
                      k(n),
                      (n.R = new Map()),
                      i(n.t, function (r) {
                        n.R.set(r, !1);
                      }),
                      n.o.clear());
                }),
                (o.forEach = function (n, r) {
                  var t = this;
                  p(this[Q]).forEach(function (e, i) {
                    n.call(r, t.get(i), i, t);
                  });
                }),
                (o.get = function (n) {
                  var r = this[Q];
                  u(r);
                  var i = p(r).get(n);
                  if (r.I || !t(i)) return i;
                  if (i !== r.t.get(n)) return i;
                  var o = N(r.A.h, i, r);
                  return e(r), r.o.set(n, o), o;
                }),
                (o.keys = function () {
                  return p(this[Q]).keys();
                }),
                (o.values = function () {
                  var n,
                    r = this,
                    t = this.keys();
                  return (
                    ((n = {})[V] = function () {
                      return r.values();
                    }),
                    (n.next = function () {
                      var n = t.next();
                      return n.done ? n : { done: !1, value: r.get(n.value) };
                    }),
                    n
                  );
                }),
                (o.entries = function () {
                  var n,
                    r = this,
                    t = this.keys();
                  return (
                    ((n = {})[V] = function () {
                      return r.entries();
                    }),
                    (n.next = function () {
                      var n = t.next();
                      if (n.done) return n;
                      var e = r.get(n.value);
                      return { done: !1, value: [n.value, e] };
                    }),
                    n
                  );
                }),
                (o[V] = function () {
                  return this.entries();
                }),
                n
              );
            })(),
            c = (function () {
              function n(n, r) {
                return (
                  (this[Q] = {
                    i: 3,
                    l: r,
                    A: r ? r.A : _(),
                    P: !1,
                    I: !1,
                    o: void 0,
                    t: n,
                    k: this,
                    p: new Map(),
                    O: !1,
                    C: !1,
                  }),
                  this
                );
              }
              r(n, Set);
              var t = n.prototype;
              return (
                Object.defineProperty(t, "size", {
                  get: function () {
                    return p(this[Q]).size;
                  },
                }),
                (t.has = function (n) {
                  var r = this[Q];
                  return (
                    u(r),
                    r.o
                      ? !!r.o.has(n) || !(!r.p.has(n) || !r.o.has(r.p.get(n)))
                      : r.t.has(n)
                  );
                }),
                (t.add = function (n) {
                  var r = this[Q];
                  return u(r), this.has(n) || (o(r), k(r), r.o.add(n)), this;
                }),
                (t.delete = function (n) {
                  if (!this.has(n)) return !1;
                  var r = this[Q];
                  return (
                    u(r),
                    o(r),
                    k(r),
                    r.o.delete(n) || (!!r.p.has(n) && r.o.delete(r.p.get(n)))
                  );
                }),
                (t.clear = function () {
                  var n = this[Q];
                  u(n), p(n).size && (o(n), k(n), n.o.clear());
                }),
                (t.values = function () {
                  var n = this[Q];
                  return u(n), o(n), n.o.values();
                }),
                (t.entries = function () {
                  var n = this[Q];
                  return u(n), o(n), n.o.entries();
                }),
                (t.keys = function () {
                  return this.values();
                }),
                (t[V] = function () {
                  return this.values();
                }),
                (t.forEach = function (n, r) {
                  for (var t = this.values(), e = t.next(); !e.done; )
                    n.call(r, e.value, e.value, this), (e = t.next());
                }),
                n
              );
            })();
          m("MapSet", {
            F: function (n, r) {
              return new f(n, r);
            },
            T: function (n, r) {
              return new c(n, r);
            },
          });
        }
        function J() {
          F(), C(), T();
        }
        function K(n) {
          return n;
        }
        function $(n) {
          return n;
        }
        var G,
          U,
          W = "undefined" != typeof Symbol && "symbol" == typeof Symbol("x"),
          X = "undefined" != typeof Map,
          q = "undefined" != typeof Set,
          B =
            "undefined" != typeof Proxy &&
            void 0 !== Proxy.revocable &&
            "undefined" != typeof Reflect,
          H = W
            ? Symbol.for("immer-nothing")
            : (((G = {})["immer-nothing"] = !0), G),
          L = W ? Symbol.for("immer-draftable") : "__$immer_draftable",
          Q = W ? Symbol.for("immer-state") : "__$immer_state",
          V = ("undefined" != typeof Symbol && Symbol.iterator) || "@@iterator",
          Y = {
            0: "Illegal state",
            1: "Immer drafts cannot have computed properties",
            2: "This object has been frozen and should not be mutated",
            3: function (n) {
              return (
                "Cannot use a proxy that has been revoked. Did you pass an object from inside an immer function to an async process? " +
                n
              );
            },
            4: "An immer producer returned a new value *and* modified its draft. Either return a new value *or* modify the draft.",
            5: "Immer forbids circular references",
            6: "The first or second argument to `produce` must be a function",
            7: "The third argument to `produce` must be a function or undefined",
            8: "First argument to `createDraft` must be a plain object, an array, or an immerable object",
            9: "First argument to `finishDraft` must be a draft returned by `createDraft`",
            10: "The given draft is already finalized",
            11: "Object.defineProperty() cannot be used on an Immer draft",
            12: "Object.setPrototypeOf() cannot be used on an Immer draft",
            13: "Immer only supports deleting array indices",
            14: "Immer only supports setting array indices and the 'length' property",
            15: function (n) {
              return "Cannot apply patch, path doesn't resolve: " + n;
            },
            16: 'Sets cannot have "replace" patches.',
            17: function (n) {
              return "Unsupported patch operation: " + n;
            },
            18: function (n) {
              return (
                "The plugin for '" +
                n +
                "' has not been loaded into Immer. To enable the plugin, import and call `enable" +
                n +
                "()` when initializing your application."
              );
            },
            20: "Cannot use proxies if Proxy, Proxy.revocable or Reflect are not available",
            21: function (n) {
              return (
                "produce can only be called on things that are draftable: plain objects, arrays, Map, Set or classes that are marked with '[immerable]: true'. Got '" +
                n +
                "'"
              );
            },
            22: function (n) {
              return "'current' expects a draft, got: " + n;
            },
            23: function (n) {
              return "'original' expects a draft, got: " + n;
            },
            24: "Patching reserved attributes like __proto__, prototype and constructor is not allowed",
          },
          Z = "" + Object.prototype.constructor,
          nn =
            "undefined" != typeof Reflect && Reflect.ownKeys
              ? Reflect.ownKeys
              : void 0 !== Object.getOwnPropertySymbols
              ? function (n) {
                  return Object.getOwnPropertyNames(n).concat(
                    Object.getOwnPropertySymbols(n)
                  );
                }
              : Object.getOwnPropertyNames,
          rn =
            Object.getOwnPropertyDescriptors ||
            function (n) {
              var r = {};
              return (
                nn(n).forEach(function (t) {
                  r[t] = Object.getOwnPropertyDescriptor(n, t);
                }),
                r
              );
            },
          tn = {},
          en = {
            get: function (n, r) {
              if (r === Q) return n;
              var e = p(n);
              if (!u(e, r))
                return (function (n, r, t) {
                  var e,
                    i = I(r, t);
                  return i
                    ? "value" in i
                      ? i.value
                      : null === (e = i.get) || void 0 === e
                      ? void 0
                      : e.call(n.k)
                    : void 0;
                })(n, e, r);
              var i = e[r];
              return n.I || !t(i)
                ? i
                : i === z(n.t, r)
                ? (E(n), (n.o[r] = N(n.A.h, i, n)))
                : i;
            },
            has: function (n, r) {
              return r in p(n);
            },
            ownKeys: function (n) {
              return Reflect.ownKeys(p(n));
            },
            set: function (n, r, t) {
              var e = I(p(n), r);
              if (null == e ? void 0 : e.set) return e.set.call(n.k, t), !0;
              if (!n.P) {
                var i = z(p(n), r),
                  o = null == i ? void 0 : i[Q];
                if (o && o.t === t) return (n.o[r] = t), (n.R[r] = !1), !0;
                if (c(t, i) && (void 0 !== t || u(n.t, r))) return !0;
                E(n), k(n);
              }
              return (
                (n.o[r] === t && (void 0 !== t || r in n.o)) ||
                  (Number.isNaN(t) && Number.isNaN(n.o[r])) ||
                  ((n.o[r] = t), (n.R[r] = !0)),
                !0
              );
            },
            deleteProperty: function (n, r) {
              return (
                void 0 !== z(n.t, r) || r in n.t
                  ? ((n.R[r] = !1), E(n), k(n))
                  : delete n.R[r],
                n.o && delete n.o[r],
                !0
              );
            },
            getOwnPropertyDescriptor: function (n, r) {
              var t = p(n),
                e = Reflect.getOwnPropertyDescriptor(t, r);
              return e
                ? {
                    writable: !0,
                    configurable: 1 !== n.i || "length" !== r,
                    enumerable: e.enumerable,
                    value: t[r],
                  }
                : e;
            },
            defineProperty: function () {
              n(11);
            },
            getPrototypeOf: function (n) {
              return Object.getPrototypeOf(n.t);
            },
            setPrototypeOf: function () {
              n(12);
            },
          },
          on = {};
        i(en, function (n, r) {
          on[n] = function () {
            return (arguments[0] = arguments[0][0]), r.apply(this, arguments);
          };
        }),
          (on.deleteProperty = function (r, t) {
            return (
              true && isNaN(parseInt(t)) && n(13),
              on.set.call(this, r, t, void 0)
            );
          }),
          (on.set = function (r, t, e) {
            return (
              true && "length" !== t && isNaN(parseInt(t)) && n(14),
              en.set.call(this, r[0], t, e, r[0])
            );
          });
        var un = (function () {
            function e(r) {
              var e = this;
              (this.g = B),
                (this.D = !0),
                (this.produce = function (r, i, o) {
                  if ("function" == typeof r && "function" != typeof i) {
                    var u = i;
                    i = r;
                    var a = e;
                    return function (n) {
                      var r = this;
                      void 0 === n && (n = u);
                      for (
                        var t = arguments.length,
                          e = Array(t > 1 ? t - 1 : 0),
                          o = 1;
                        o < t;
                        o++
                      )
                        e[o - 1] = arguments[o];
                      return a.produce(n, function (n) {
                        var t;
                        return (t = i).call.apply(t, [r, n].concat(e));
                      });
                    };
                  }
                  var f;
                  if (
                    ("function" != typeof i && n(6),
                    void 0 !== o && "function" != typeof o && n(7),
                    t(r))
                  ) {
                    var c = w(e),
                      s = N(e, r, void 0),
                      v = !0;
                    try {
                      (f = i(s)), (v = !1);
                    } finally {
                      v ? O(c) : g(c);
                    }
                    return "undefined" != typeof Promise && f instanceof Promise
                      ? f.then(
                          function (n) {
                            return j(c, o), P(n, c);
                          },
                          function (n) {
                            throw (O(c), n);
                          }
                        )
                      : (j(c, o), P(f, c));
                  }
                  if (!r || "object" != typeof r) {
                    if (
                      (void 0 === (f = i(r)) && (f = r),
                      f === H && (f = void 0),
                      e.D && d(f, !0),
                      o)
                    ) {
                      var p = [],
                        l = [];
                      b("Patches").M(r, f, p, l), o(p, l);
                    }
                    return f;
                  }
                  n(21, r);
                }),
                (this.produceWithPatches = function (n, r) {
                  if ("function" == typeof n)
                    return function (r) {
                      for (
                        var t = arguments.length,
                          i = Array(t > 1 ? t - 1 : 0),
                          o = 1;
                        o < t;
                        o++
                      )
                        i[o - 1] = arguments[o];
                      return e.produceWithPatches(r, function (r) {
                        return n.apply(void 0, [r].concat(i));
                      });
                    };
                  var t,
                    i,
                    o = e.produce(n, r, function (n, r) {
                      (t = n), (i = r);
                    });
                  return "undefined" != typeof Promise && o instanceof Promise
                    ? o.then(function (n) {
                        return [n, t, i];
                      })
                    : [o, t, i];
                }),
                "boolean" == typeof (null == r ? void 0 : r.useProxies) &&
                  this.setUseProxies(r.useProxies),
                "boolean" == typeof (null == r ? void 0 : r.autoFreeze) &&
                  this.setAutoFreeze(r.autoFreeze);
            }
            var i = e.prototype;
            return (
              (i.createDraft = function (e) {
                t(e) || n(8), r(e) && (e = R(e));
                var i = w(this),
                  o = N(this, e, void 0);
                return (o[Q].C = !0), g(i), o;
              }),
              (i.finishDraft = function (r, t) {
                var e = r && r[Q];
                true && ((e && e.C) || n(9), e.I && n(10));
                var i = e.A;
                return j(i, t), P(void 0, i);
              }),
              (i.setAutoFreeze = function (n) {
                this.D = n;
              }),
              (i.setUseProxies = function (r) {
                r && !B && n(20), (this.g = r);
              }),
              (i.applyPatches = function (n, t) {
                var e;
                for (e = t.length - 1; e >= 0; e--) {
                  var i = t[e];
                  if (0 === i.path.length && "replace" === i.op) {
                    n = i.value;
                    break;
                  }
                }
                e > -1 && (t = t.slice(e + 1));
                var o = b("Patches").$;
                return r(n)
                  ? o(n, t)
                  : this.produce(n, function (n) {
                      return o(n, t);
                    });
              }),
              e
            );
          })(),
          an = new un(),
          fn = an.produce,
          cn = an.produceWithPatches.bind(an),
          sn = an.setAutoFreeze.bind(an),
          vn = an.setUseProxies.bind(an),
          pn = an.applyPatches.bind(an),
          ln = an.createDraft.bind(an),
          dn = an.finishDraft.bind(an);
        /* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = fn;
        //# sourceMappingURL=immer.esm.js.map

        /***/
      },

    /***/ "./node_modules/lit-html/development/lit-html.js":
      /*!*******************************************************!*\
  !*** ./node_modules/lit-html/development/lit-html.js ***!
  \*******************************************************/
      /***/ (
        __unused_webpack___webpack_module__,
        __webpack_exports__,
        __webpack_require__
      ) => {
        __webpack_require__.r(__webpack_exports__);
        /* harmony export */ __webpack_require__.d(__webpack_exports__, {
          /* harmony export */ _$LH: () => /* binding */ _$LH,
          /* harmony export */ html: () => /* binding */ html,
          /* harmony export */ noChange: () => /* binding */ noChange,
          /* harmony export */ nothing: () => /* binding */ nothing,
          /* harmony export */ render: () => /* binding */ render,
          /* harmony export */ svg: () => /* binding */ svg,
          /* harmony export */
        });
        /**
         * @license
         * Copyright 2017 Google LLC
         * SPDX-License-Identifier: BSD-3-Clause
         */
        var _a, _b, _c, _d;
        const DEV_MODE = true;
        const ENABLE_EXTRA_SECURITY_HOOKS = true;
        const ENABLE_SHADYDOM_NOPATCH = true;
        const NODE_MODE = false;
        // Use window for browser builds because IE11 doesn't have globalThis.
        const global = NODE_MODE ? globalThis : window;
        /**
         * Useful for visualizing and logging insights into what the Lit template system is doing.
         *
         * Compiled out of prod mode builds.
         */
        const debugLogEvent = DEV_MODE
          ? (event) => {
              const shouldEmit = global.emitLitDebugLogEvents;
              if (!shouldEmit) {
                return;
              }
              global.dispatchEvent(
                new CustomEvent("lit-debug", {
                  detail: event,
                })
              );
            }
          : undefined;
        // Used for connecting beginRender and endRender events when there are nested
        // renders when errors are thrown preventing an endRender event from being
        // called.
        let debugLogRenderId = 0;
        let issueWarning;
        if (DEV_MODE) {
          (_a = global.litIssuedWarnings) !== null && _a !== void 0
            ? _a
            : (global.litIssuedWarnings = new Set());
          // Issue a warning, if we haven't already.
          issueWarning = (code, warning) => {
            warning += code
              ? ` See https://lit.dev/msg/${code} for more information.`
              : "";
            if (!global.litIssuedWarnings.has(warning)) {
              console.warn(warning);
              global.litIssuedWarnings.add(warning);
            }
          };
          issueWarning(
            "dev-mode",
            `Lit is in dev mode. Not recommended for production!`
          );
        }
        const wrap =
          ENABLE_SHADYDOM_NOPATCH &&
          ((_b = global.ShadyDOM) === null || _b === void 0
            ? void 0
            : _b.inUse) &&
          ((_c = global.ShadyDOM) === null || _c === void 0
            ? void 0
            : _c.noPatch) === true
            ? global.ShadyDOM.wrap
            : (node) => node;
        const trustedTypes = global.trustedTypes;
        /**
         * Our TrustedTypePolicy for HTML which is declared using the html template
         * tag function.
         *
         * That HTML is a developer-authored constant, and is parsed with innerHTML
         * before any untrusted expressions have been mixed in. Therefor it is
         * considered safe by construction.
         */
        const policy = trustedTypes
          ? trustedTypes.createPolicy("lit-html", {
              createHTML: (s) => s,
            })
          : undefined;
        const identityFunction = (value) => value;
        const noopSanitizer = (_node, _name, _type) => identityFunction;
        /** Sets the global sanitizer factory. */
        const setSanitizer = (newSanitizer) => {
          if (!ENABLE_EXTRA_SECURITY_HOOKS) {
            return;
          }
          if (sanitizerFactoryInternal !== noopSanitizer) {
            throw new Error(
              `Attempted to overwrite existing lit-html security policy.` +
                ` setSanitizeDOMValueFactory should be called at most once.`
            );
          }
          sanitizerFactoryInternal = newSanitizer;
        };
        /**
         * Only used in internal tests, not a part of the public API.
         */
        const _testOnlyClearSanitizerFactoryDoNotCallOrElse = () => {
          sanitizerFactoryInternal = noopSanitizer;
        };
        const createSanitizer = (node, name, type) => {
          return sanitizerFactoryInternal(node, name, type);
        };
        // Added to an attribute name to mark the attribute as bound so we can find
        // it easily.
        const boundAttributeSuffix = "$lit$";
        // This marker is used in many syntactic positions in HTML, so it must be
        // a valid element name and attribute name. We don't support dynamic names (yet)
        // but this at least ensures that the parse tree is closer to the template
        // intention.
        const marker = `lit$${String(Math.random()).slice(9)}$`;
        // String used to tell if a comment is a marker comment
        const markerMatch = "?" + marker;
        // Text used to insert a comment marker node. We use processing instruction
        // syntax because it's slightly smaller, but parses as a comment node.
        const nodeMarker = `<${markerMatch}>`;
        const d =
          NODE_MODE && global.document === undefined
            ? {
                createTreeWalker() {
                  return {};
                },
              }
            : document;
        // Creates a dynamic marker. We never have to search for these in the DOM.
        const createMarker = (v = "") => d.createComment(v);
        const isPrimitive = (value) =>
          value === null ||
          (typeof value != "object" && typeof value != "function");
        const isArray = Array.isArray;
        const isIterable = (value) =>
          isArray(value) ||
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          typeof (value === null || value === void 0
            ? void 0
            : value[Symbol.iterator]) === "function";
        const SPACE_CHAR = `[ \t\n\f\r]`;
        const ATTR_VALUE_CHAR = `[^ \t\n\f\r"'\`<>=]`;
        const NAME_CHAR = `[^\\s"'>=/]`;
        // These regexes represent the five parsing states that we care about in the
        // Template's HTML scanner. They match the *end* of the state they're named
        // after.
        // Depending on the match, we transition to a new state. If there's no match,
        // we stay in the same state.
        // Note that the regexes are stateful. We utilize lastIndex and sync it
        // across the multiple regexes used. In addition to the five regexes below
        // we also dynamically create a regex to find the matching end tags for raw
        // text elements.
        /**
         * End of text is: `<` followed by:
         *   (comment start) or (tag) or (dynamic tag binding)
         */
        const textEndRegex =
          /<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g;
        const COMMENT_START = 1;
        const TAG_NAME = 2;
        const DYNAMIC_TAG_NAME = 3;
        const commentEndRegex = /-->/g;
        /**
         * Comments not started with <!--, like </{, can be ended by a single `>`
         */
        const comment2EndRegex = />/g;
        /**
         * The tagEnd regex matches the end of the "inside an opening" tag syntax
         * position. It either matches a `>`, an attribute-like sequence, or the end
         * of the string after a space (attribute-name position ending).
         *
         * See attributes in the HTML spec:
         * https://www.w3.org/TR/html5/syntax.html#elements-attributes
         *
         * " \t\n\f\r" are HTML space characters:
         * https://infra.spec.whatwg.org/#ascii-whitespace
         *
         * So an attribute is:
         *  * The name: any character except a whitespace character, ("), ('), ">",
         *    "=", or "/". Note: this is different from the HTML spec which also excludes control characters.
         *  * Followed by zero or more space characters
         *  * Followed by "="
         *  * Followed by zero or more space characters
         *  * Followed by:
         *    * Any character except space, ('), ("), "<", ">", "=", (`), or
         *    * (") then any non-("), or
         *    * (') then any non-(')
         */
        const tagEndRegex = new RegExp(
          `>|${SPACE_CHAR}(?:(${NAME_CHAR}+)(${SPACE_CHAR}*=${SPACE_CHAR}*(?:${ATTR_VALUE_CHAR}|("|')|))|$)`,
          "g"
        );
        const ENTIRE_MATCH = 0;
        const ATTRIBUTE_NAME = 1;
        const SPACES_AND_EQUALS = 2;
        const QUOTE_CHAR = 3;
        const singleQuoteAttrEndRegex = /'/g;
        const doubleQuoteAttrEndRegex = /"/g;
        /**
         * Matches the raw text elements.
         *
         * Comments are not parsed within raw text elements, so we need to search their
         * text content for marker strings.
         */
        const rawTextElement = /^(?:script|style|textarea|title)$/i;
        /** TemplateResult types */
        const HTML_RESULT = 1;
        const SVG_RESULT = 2;
        // TemplatePart types
        // IMPORTANT: these must match the values in PartType
        const ATTRIBUTE_PART = 1;
        const CHILD_PART = 2;
        const PROPERTY_PART = 3;
        const BOOLEAN_ATTRIBUTE_PART = 4;
        const EVENT_PART = 5;
        const ELEMENT_PART = 6;
        const COMMENT_PART = 7;
        /**
         * Generates a template literal tag function that returns a TemplateResult with
         * the given result type.
         */
        const tag =
          (type) =>
          (strings, ...values) => {
            // Warn against templates octal escape sequences
            // We do this here rather than in render so that the warning is closer to the
            // template definition.
            if (DEV_MODE && strings.some((s) => s === undefined)) {
              console.warn(
                "Some template strings are undefined.\n" +
                  "This is probably caused by illegal octal escape sequences."
              );
            }
            return {
              // This property needs to remain unminified.
              ["_$litType$"]: type,
              strings,
              values,
            };
          };
        /**
         * Interprets a template literal as an HTML template that can efficiently
         * render to and update a container.
         *
         * ```ts
         * const header = (title: string) => html`<h1>${title}</h1>`;
         * ```
         *
         * The `html` tag returns a description of the DOM to render as a value. It is
         * lazy, meaning no work is done until the template is rendered. When rendering,
         * if a template comes from the same expression as a previously rendered result,
         * it's efficiently updated instead of replaced.
         */
        const html = tag(HTML_RESULT);
        /**
         * Interprets a template literal as an SVG fragment that can efficiently
         * render to and update a container.
         *
         * ```ts
         * const rect = svg`<rect width="10" height="10"></rect>`;
         *
         * const myImage = html`
         *   <svg viewBox="0 0 10 10" xmlns="http://www.w3.org/2000/svg">
         *     ${rect}
         *   </svg>`;
         * ```
         *
         * The `svg` *tag function* should only be used for SVG fragments, or elements
         * that would be contained **inside** an `<svg>` HTML element. A common error is
         * placing an `<svg>` *element* in a template tagged with the `svg` tag
         * function. The `<svg>` element is an HTML element and should be used within a
         * template tagged with the {@linkcode html} tag function.
         *
         * In LitElement usage, it's invalid to return an SVG fragment from the
         * `render()` method, as the SVG fragment will be contained within the element's
         * shadow root and thus cannot be used within an `<svg>` HTML element.
         */
        const svg = tag(SVG_RESULT);
        /**
         * A sentinel value that signals that a value was handled by a directive and
         * should not be written to the DOM.
         */
        const noChange = Symbol.for("lit-noChange");
        /**
         * A sentinel value that signals a ChildPart to fully clear its content.
         *
         * ```ts
         * const button = html`${
         *  user.isAdmin
         *    ? html`<button>DELETE</button>`
         *    : nothing
         * }`;
         * ```
         *
         * Prefer using `nothing` over other falsy values as it provides a consistent
         * behavior between various expression binding contexts.
         *
         * In child expressions, `undefined`, `null`, `''`, and `nothing` all behave the
         * same and render no nodes. In attribute expressions, `nothing` _removes_ the
         * attribute, while `undefined` and `null` will render an empty string. In
         * property expressions `nothing` becomes `undefined`.
         */
        const nothing = Symbol.for("lit-nothing");
        /**
         * The cache of prepared templates, keyed by the tagged TemplateStringsArray
         * and _not_ accounting for the specific template tag used. This means that
         * template tags cannot be dynamic - the must statically be one of html, svg,
         * or attr. This restriction simplifies the cache lookup, which is on the hot
         * path for rendering.
         */
        const templateCache = new WeakMap();
        const walker = d.createTreeWalker(
          d,
          129 /* NodeFilter.SHOW_{ELEMENT|COMMENT} */,
          null,
          false
        );
        let sanitizerFactoryInternal = noopSanitizer;
        /**
         * Returns an HTML string for the given TemplateStringsArray and result type
         * (HTML or SVG), along with the case-sensitive bound attribute names in
         * template order. The HTML contains comment markers denoting the `ChildPart`s
         * and suffixes on bound attributes denoting the `AttributeParts`.
         *
         * @param strings template strings array
         * @param type HTML or SVG
         * @return Array containing `[html, attrNames]` (array returned for terseness,
         *     to avoid object fields since this code is shared with non-minified SSR
         *     code)
         */
        const getTemplateHtml = (strings, type) => {
          // Insert makers into the template HTML to represent the position of
          // bindings. The following code scans the template strings to determine the
          // syntactic position of the bindings. They can be in text position, where
          // we insert an HTML comment, attribute value position, where we insert a
          // sentinel string and re-write the attribute name, or inside a tag where
          // we insert the sentinel string.
          const l = strings.length - 1;
          // Stores the case-sensitive bound attribute names in the order of their
          // parts. ElementParts are also reflected in this array as undefined
          // rather than a string, to disambiguate from attribute bindings.
          const attrNames = [];
          let html = type === SVG_RESULT ? "<svg>" : "";
          // When we're inside a raw text tag (not it's text content), the regex
          // will still be tagRegex so we can find attributes, but will switch to
          // this regex when the tag ends.
          let rawTextEndRegex;
          // The current parsing state, represented as a reference to one of the
          // regexes
          let regex = textEndRegex;
          for (let i = 0; i < l; i++) {
            const s = strings[i];
            // The index of the end of the last attribute name. When this is
            // positive at end of a string, it means we're in an attribute value
            // position and need to rewrite the attribute name.
            // We also use a special value of -2 to indicate that we encountered
            // the end of a string in attribute name position.
            let attrNameEndIndex = -1;
            let attrName;
            let lastIndex = 0;
            let match;
            // The conditions in this loop handle the current parse state, and the
            // assignments to the `regex` variable are the state transitions.
            while (lastIndex < s.length) {
              // Make sure we start searching from where we previously left off
              regex.lastIndex = lastIndex;
              match = regex.exec(s);
              if (match === null) {
                break;
              }
              lastIndex = regex.lastIndex;
              if (regex === textEndRegex) {
                if (match[COMMENT_START] === "!--") {
                  regex = commentEndRegex;
                } else if (match[COMMENT_START] !== undefined) {
                  // We started a weird comment, like </{
                  regex = comment2EndRegex;
                } else if (match[TAG_NAME] !== undefined) {
                  if (rawTextElement.test(match[TAG_NAME])) {
                    // Record if we encounter a raw-text element. We'll switch to
                    // this regex at the end of the tag.
                    rawTextEndRegex = new RegExp(`</${match[TAG_NAME]}`, "g");
                  }
                  regex = tagEndRegex;
                } else if (match[DYNAMIC_TAG_NAME] !== undefined) {
                  if (DEV_MODE) {
                    throw new Error(
                      "Bindings in tag names are not supported. Please use static templates instead. " +
                        "See https://lit.dev/docs/templates/expressions/#static-expressions"
                    );
                  }
                  regex = tagEndRegex;
                }
              } else if (regex === tagEndRegex) {
                if (match[ENTIRE_MATCH] === ">") {
                  // End of a tag. If we had started a raw-text element, use that
                  // regex
                  regex =
                    rawTextEndRegex !== null && rawTextEndRegex !== void 0
                      ? rawTextEndRegex
                      : textEndRegex;
                  // We may be ending an unquoted attribute value, so make sure we
                  // clear any pending attrNameEndIndex
                  attrNameEndIndex = -1;
                } else if (match[ATTRIBUTE_NAME] === undefined) {
                  // Attribute name position
                  attrNameEndIndex = -2;
                } else {
                  attrNameEndIndex =
                    regex.lastIndex - match[SPACES_AND_EQUALS].length;
                  attrName = match[ATTRIBUTE_NAME];
                  regex =
                    match[QUOTE_CHAR] === undefined
                      ? tagEndRegex
                      : match[QUOTE_CHAR] === '"'
                      ? doubleQuoteAttrEndRegex
                      : singleQuoteAttrEndRegex;
                }
              } else if (
                regex === doubleQuoteAttrEndRegex ||
                regex === singleQuoteAttrEndRegex
              ) {
                regex = tagEndRegex;
              } else if (
                regex === commentEndRegex ||
                regex === comment2EndRegex
              ) {
                regex = textEndRegex;
              } else {
                // Not one of the five state regexes, so it must be the dynamically
                // created raw text regex and we're at the close of that element.
                regex = tagEndRegex;
                rawTextEndRegex = undefined;
              }
            }
            if (DEV_MODE) {
              // If we have a attrNameEndIndex, which indicates that we should
              // rewrite the attribute name, assert that we're in a valid attribute
              // position - either in a tag, or a quoted attribute value.
              console.assert(
                attrNameEndIndex === -1 ||
                  regex === tagEndRegex ||
                  regex === singleQuoteAttrEndRegex ||
                  regex === doubleQuoteAttrEndRegex,
                "unexpected parse state B"
              );
            }
            // We have four cases:
            //  1. We're in text position, and not in a raw text element
            //     (regex === textEndRegex): insert a comment marker.
            //  2. We have a non-negative attrNameEndIndex which means we need to
            //     rewrite the attribute name to add a bound attribute suffix.
            //  3. We're at the non-first binding in a multi-binding attribute, use a
            //     plain marker.
            //  4. We're somewhere else inside the tag. If we're in attribute name
            //     position (attrNameEndIndex === -2), add a sequential suffix to
            //     generate a unique attribute name.
            // Detect a binding next to self-closing tag end and insert a space to
            // separate the marker from the tag end:
            const end =
              regex === tagEndRegex && strings[i + 1].startsWith("/>")
                ? " "
                : "";
            html +=
              regex === textEndRegex
                ? s + nodeMarker
                : attrNameEndIndex >= 0
                ? (attrNames.push(attrName),
                  s.slice(0, attrNameEndIndex) +
                    boundAttributeSuffix +
                    s.slice(attrNameEndIndex)) +
                  marker +
                  end
                : s +
                  marker +
                  (attrNameEndIndex === -2
                    ? (attrNames.push(undefined), i)
                    : end);
          }
          const htmlResult =
            html +
            (strings[l] || "<?>") +
            (type === SVG_RESULT ? "</svg>" : "");
          // A security check to prevent spoofing of Lit template results.
          // In the future, we may be able to replace this with Array.isTemplateObject,
          // though we might need to make that check inside of the html and svg
          // functions, because precompiled templates don't come in as
          // TemplateStringArray objects.
          if (!Array.isArray(strings) || !strings.hasOwnProperty("raw")) {
            let message = "invalid template strings array";
            if (DEV_MODE) {
              message = `
          Internal Error: expected template strings to be an array
          with a 'raw' field. Faking a template strings array by
          calling html or svg like an ordinary function is effectively
          the same as calling unsafeHtml and can lead to major security
          issues, e.g. opening your code up to XSS attacks.

          If you're using the html or svg tagged template functions normally
          and and still seeing this error, please file a bug at
          https://github.com/lit/lit/issues/new?template=bug_report.md
          and include information about your build tooling, if any.
        `
                .trim()
                .replace(/\n */g, "\n");
            }
            throw new Error(message);
          }
          // Returned as an array for terseness
          return [
            policy !== undefined ? policy.createHTML(htmlResult) : htmlResult,
            attrNames,
          ];
        };
        class Template {
          constructor(
            // This property needs to remain unminified.
            { strings, ["_$litType$"]: type },
            options
          ) {
            /** @internal */
            this.parts = [];
            let node;
            let nodeIndex = 0;
            let attrNameIndex = 0;
            const partCount = strings.length - 1;
            const parts = this.parts;
            // Create template element
            const [html, attrNames] = getTemplateHtml(strings, type);
            this.el = Template.createElement(html, options);
            walker.currentNode = this.el.content;
            // Reparent SVG nodes into template root
            if (type === SVG_RESULT) {
              const content = this.el.content;
              const svgElement = content.firstChild;
              svgElement.remove();
              content.append(...svgElement.childNodes);
            }
            // Walk the template to find binding markers and create TemplateParts
            while (
              (node = walker.nextNode()) !== null &&
              parts.length < partCount
            ) {
              if (node.nodeType === 1) {
                if (DEV_MODE) {
                  const tag = node.localName;
                  // Warn if `textarea` includes an expression and throw if `template`
                  // does since these are not supported. We do this by checking
                  // innerHTML for anything that looks like a marker. This catches
                  // cases like bindings in textarea there markers turn into text nodes.
                  if (
                    /^(?:textarea|template)$/i.test(tag) &&
                    node.innerHTML.includes(marker)
                  ) {
                    const m =
                      `Expressions are not supported inside \`${tag}\` ` +
                      `elements. See https://lit.dev/msg/expression-in-${tag} for more ` +
                      `information.`;
                    if (tag === "template") {
                      throw new Error(m);
                    } else issueWarning("", m);
                  }
                }
                // TODO (justinfagnani): for attempted dynamic tag names, we don't
                // increment the bindingIndex, and it'll be off by 1 in the element
                // and off by two after it.
                if (node.hasAttributes()) {
                  // We defer removing bound attributes because on IE we might not be
                  // iterating attributes in their template order, and would sometimes
                  // remove an attribute that we still need to create a part for.
                  const attrsToRemove = [];
                  for (const name of node.getAttributeNames()) {
                    // `name` is the name of the attribute we're iterating over, but not
                    // _neccessarily_ the name of the attribute we will create a part
                    // for. They can be different in browsers that don't iterate on
                    // attributes in source order. In that case the attrNames array
                    // contains the attribute name we'll process next. We only need the
                    // attribute name here to know if we should process a bound attribute
                    // on this element.
                    if (
                      name.endsWith(boundAttributeSuffix) ||
                      name.startsWith(marker)
                    ) {
                      const realName = attrNames[attrNameIndex++];
                      attrsToRemove.push(name);
                      if (realName !== undefined) {
                        // Lowercase for case-sensitive SVG attributes like viewBox
                        const value = node.getAttribute(
                          realName.toLowerCase() + boundAttributeSuffix
                        );
                        const statics = value.split(marker);
                        const m = /([.?@])?(.*)/.exec(realName);
                        parts.push({
                          type: ATTRIBUTE_PART,
                          index: nodeIndex,
                          name: m[2],
                          strings: statics,
                          ctor:
                            m[1] === "."
                              ? PropertyPart
                              : m[1] === "?"
                              ? BooleanAttributePart
                              : m[1] === "@"
                              ? EventPart
                              : AttributePart,
                        });
                      } else {
                        parts.push({
                          type: ELEMENT_PART,
                          index: nodeIndex,
                        });
                      }
                    }
                  }
                  for (const name of attrsToRemove) {
                    node.removeAttribute(name);
                  }
                }
                // TODO (justinfagnani): benchmark the regex against testing for each
                // of the 3 raw text element names.
                if (rawTextElement.test(node.tagName)) {
                  // For raw text elements we need to split the text content on
                  // markers, create a Text node for each segment, and create
                  // a TemplatePart for each marker.
                  const strings = node.textContent.split(marker);
                  const lastIndex = strings.length - 1;
                  if (lastIndex > 0) {
                    node.textContent = trustedTypes
                      ? trustedTypes.emptyScript
                      : "";
                    // Generate a new text node for each literal section
                    // These nodes are also used as the markers for node parts
                    // We can't use empty text nodes as markers because they're
                    // normalized when cloning in IE (could simplify when
                    // IE is no longer supported)
                    for (let i = 0; i < lastIndex; i++) {
                      node.append(strings[i], createMarker());
                      // Walk past the marker node we just added
                      walker.nextNode();
                      parts.push({ type: CHILD_PART, index: ++nodeIndex });
                    }
                    // Note because this marker is added after the walker's current
                    // node, it will be walked to in the outer loop (and ignored), so
                    // we don't need to adjust nodeIndex here
                    node.append(strings[lastIndex], createMarker());
                  }
                }
              } else if (node.nodeType === 8) {
                const data = node.data;
                if (data === markerMatch) {
                  parts.push({ type: CHILD_PART, index: nodeIndex });
                } else {
                  let i = -1;
                  while ((i = node.data.indexOf(marker, i + 1)) !== -1) {
                    // Comment node has a binding marker inside, make an inactive part
                    // The binding won't work, but subsequent bindings will
                    parts.push({ type: COMMENT_PART, index: nodeIndex });
                    // Move to the end of the match
                    i += marker.length - 1;
                  }
                }
              }
              nodeIndex++;
            }
            debugLogEvent === null || debugLogEvent === void 0
              ? void 0
              : debugLogEvent({
                  kind: "template prep",
                  template: this,
                  clonableTemplate: this.el,
                  parts: this.parts,
                  strings,
                });
          }
          // Overridden via `litHtmlPolyfillSupport` to provide platform support.
          /** @nocollapse */
          static createElement(html, _options) {
            const el = d.createElement("template");
            el.innerHTML = html;
            return el;
          }
        }
        function resolveDirective(part, value, parent = part, attributeIndex) {
          var _a, _b, _c;
          var _d;
          // Bail early if the value is explicitly noChange. Note, this means any
          // nested directive is still attached and is not run.
          if (value === noChange) {
            return value;
          }
          let currentDirective =
            attributeIndex !== undefined
              ? (_a = parent.__directives) === null || _a === void 0
                ? void 0
                : _a[attributeIndex]
              : parent.__directive;
          const nextDirectiveConstructor = isPrimitive(value)
            ? undefined
            : // This property needs to remain unminified.
              value["_$litDirective$"];
          if (
            (currentDirective === null || currentDirective === void 0
              ? void 0
              : currentDirective.constructor) !== nextDirectiveConstructor
          ) {
            // This property needs to remain unminified.
            (_b =
              currentDirective === null || currentDirective === void 0
                ? void 0
                : currentDirective["_$notifyDirectiveConnectionChanged"]) ===
              null || _b === void 0
              ? void 0
              : _b.call(currentDirective, false);
            if (nextDirectiveConstructor === undefined) {
              currentDirective = undefined;
            } else {
              currentDirective = new nextDirectiveConstructor(part);
              currentDirective._$initialize(part, parent, attributeIndex);
            }
            if (attributeIndex !== undefined) {
              ((_c = (_d = parent).__directives) !== null && _c !== void 0
                ? _c
                : (_d.__directives = []))[attributeIndex] = currentDirective;
            } else {
              parent.__directive = currentDirective;
            }
          }
          if (currentDirective !== undefined) {
            value = resolveDirective(
              part,
              currentDirective._$resolve(part, value.values),
              currentDirective,
              attributeIndex
            );
          }
          return value;
        }
        /**
         * An updateable instance of a Template. Holds references to the Parts used to
         * update the template instance.
         */
        class TemplateInstance {
          constructor(template, parent) {
            /** @internal */
            this._parts = [];
            /** @internal */
            this._$disconnectableChildren = undefined;
            this._$template = template;
            this._$parent = parent;
          }
          // Called by ChildPart parentNode getter
          get parentNode() {
            return this._$parent.parentNode;
          }
          // See comment in Disconnectable interface for why this is a getter
          get _$isConnected() {
            return this._$parent._$isConnected;
          }
          // This method is separate from the constructor because we need to return a
          // DocumentFragment and we don't want to hold onto it with an instance field.
          _clone(options) {
            var _a;
            const {
              el: { content },
              parts: parts,
            } = this._$template;
            const fragment = (
              (_a =
                options === null || options === void 0
                  ? void 0
                  : options.creationScope) !== null && _a !== void 0
                ? _a
                : d
            ).importNode(content, true);
            walker.currentNode = fragment;
            let node = walker.nextNode();
            let nodeIndex = 0;
            let partIndex = 0;
            let templatePart = parts[0];
            while (templatePart !== undefined) {
              if (nodeIndex === templatePart.index) {
                let part;
                if (templatePart.type === CHILD_PART) {
                  part = new ChildPart(node, node.nextSibling, this, options);
                } else if (templatePart.type === ATTRIBUTE_PART) {
                  part = new templatePart.ctor(
                    node,
                    templatePart.name,
                    templatePart.strings,
                    this,
                    options
                  );
                } else if (templatePart.type === ELEMENT_PART) {
                  part = new ElementPart(node, this, options);
                }
                this._parts.push(part);
                templatePart = parts[++partIndex];
              }
              if (
                nodeIndex !==
                (templatePart === null || templatePart === void 0
                  ? void 0
                  : templatePart.index)
              ) {
                node = walker.nextNode();
                nodeIndex++;
              }
            }
            return fragment;
          }
          _update(values) {
            let i = 0;
            for (const part of this._parts) {
              if (part !== undefined) {
                debugLogEvent === null || debugLogEvent === void 0
                  ? void 0
                  : debugLogEvent({
                      kind: "set part",
                      part,
                      value: values[i],
                      valueIndex: i,
                      values,
                      templateInstance: this,
                    });
                if (part.strings !== undefined) {
                  part._$setValue(values, part, i);
                  // The number of values the part consumes is part.strings.length - 1
                  // since values are in between template spans. We increment i by 1
                  // later in the loop, so increment it by part.strings.length - 2 here
                  i += part.strings.length - 2;
                } else {
                  part._$setValue(values[i]);
                }
              }
              i++;
            }
          }
        }
        class ChildPart {
          constructor(startNode, endNode, parent, options) {
            var _a;
            this.type = CHILD_PART;
            this._$committedValue = nothing;
            // The following fields will be patched onto ChildParts when required by
            // AsyncDirective
            /** @internal */
            this._$disconnectableChildren = undefined;
            this._$startNode = startNode;
            this._$endNode = endNode;
            this._$parent = parent;
            this.options = options;
            // Note __isConnected is only ever accessed on RootParts (i.e. when there is
            // no _$parent); the value on a non-root-part is "don't care", but checking
            // for parent would be more code
            this.__isConnected =
              (_a =
                options === null || options === void 0
                  ? void 0
                  : options.isConnected) !== null && _a !== void 0
                ? _a
                : true;
            if (ENABLE_EXTRA_SECURITY_HOOKS) {
              // Explicitly initialize for consistent class shape.
              this._textSanitizer = undefined;
            }
          }
          // See comment in Disconnectable interface for why this is a getter
          get _$isConnected() {
            var _a, _b;
            // ChildParts that are not at the root should always be created with a
            // parent; only RootChildNode's won't, so they return the local isConnected
            // state
            return (_b =
              (_a = this._$parent) === null || _a === void 0
                ? void 0
                : _a._$isConnected) !== null && _b !== void 0
              ? _b
              : this.__isConnected;
          }
          /**
           * The parent node into which the part renders its content.
           *
           * A ChildPart's content consists of a range of adjacent child nodes of
           * `.parentNode`, possibly bordered by 'marker nodes' (`.startNode` and
           * `.endNode`).
           *
           * - If both `.startNode` and `.endNode` are non-null, then the part's content
           * consists of all siblings between `.startNode` and `.endNode`, exclusively.
           *
           * - If `.startNode` is non-null but `.endNode` is null, then the part's
           * content consists of all siblings following `.startNode`, up to and
           * including the last child of `.parentNode`. If `.endNode` is non-null, then
           * `.startNode` will always be non-null.
           *
           * - If both `.endNode` and `.startNode` are null, then the part's content
           * consists of all child nodes of `.parentNode`.
           */
          get parentNode() {
            let parentNode = wrap(this._$startNode).parentNode;
            const parent = this._$parent;
            if (
              parent !== undefined &&
              parentNode.nodeType === 11 /* Node.DOCUMENT_FRAGMENT */
            ) {
              // If the parentNode is a DocumentFragment, it may be because the DOM is
              // still in the cloned fragment during initial render; if so, get the real
              // parentNode the part will be committed into by asking the parent.
              parentNode = parent.parentNode;
            }
            return parentNode;
          }
          /**
           * The part's leading marker node, if any. See `.parentNode` for more
           * information.
           */
          get startNode() {
            return this._$startNode;
          }
          /**
           * The part's trailing marker node, if any. See `.parentNode` for more
           * information.
           */
          get endNode() {
            return this._$endNode;
          }
          _$setValue(value, directiveParent = this) {
            var _a;
            if (DEV_MODE && this.parentNode === null) {
              throw new Error(
                `This \`ChildPart\` has no \`parentNode\` and therefore cannot accept a value. This likely means the element containing the part was manipulated in an unsupported way outside of Lit's control such that the part's marker nodes were ejected from DOM. For example, setting the element's \`innerHTML\` or \`textContent\` can do this.`
              );
            }
            value = resolveDirective(this, value, directiveParent);
            if (isPrimitive(value)) {
              // Non-rendering child values. It's important that these do not render
              // empty text nodes to avoid issues with preventing default <slot>
              // fallback content.
              if (value === nothing || value == null || value === "") {
                if (this._$committedValue !== nothing) {
                  debugLogEvent === null || debugLogEvent === void 0
                    ? void 0
                    : debugLogEvent({
                        kind: "commit nothing to child",
                        start: this._$startNode,
                        end: this._$endNode,
                        parent: this._$parent,
                        options: this.options,
                      });
                  this._$clear();
                }
                this._$committedValue = nothing;
              } else if (
                value !== this._$committedValue &&
                value !== noChange
              ) {
                this._commitText(value);
              }
              // This property needs to remain unminified.
            } else if (value["_$litType$"] !== undefined) {
              this._commitTemplateResult(value);
            } else if (value.nodeType !== undefined) {
              if (
                DEV_MODE &&
                ((_a = this.options) === null || _a === void 0
                  ? void 0
                  : _a.host) === value
              ) {
                this._commitText(
                  `[probable mistake: rendered a template's host in itself ` +
                    `(commonly caused by writing \${this} in a template]`
                );
                console.warn(
                  `Attempted to render the template host`,
                  value,
                  `inside itself. This is almost always a mistake, and in dev mode `,
                  `we render some warning text. In production however, we'll `,
                  `render it, which will usually result in an error, and sometimes `,
                  `in the element disappearing from the DOM.`
                );
                return;
              }
              this._commitNode(value);
            } else if (isIterable(value)) {
              this._commitIterable(value);
            } else {
              // Fallback, will render the string representation
              this._commitText(value);
            }
          }
          _insert(node, ref = this._$endNode) {
            return wrap(wrap(this._$startNode).parentNode).insertBefore(
              node,
              ref
            );
          }
          _commitNode(value) {
            var _a;
            if (this._$committedValue !== value) {
              this._$clear();
              if (
                ENABLE_EXTRA_SECURITY_HOOKS &&
                sanitizerFactoryInternal !== noopSanitizer
              ) {
                const parentNodeName =
                  (_a = this._$startNode.parentNode) === null || _a === void 0
                    ? void 0
                    : _a.nodeName;
                if (parentNodeName === "STYLE" || parentNodeName === "SCRIPT") {
                  let message = "Forbidden";
                  if (DEV_MODE) {
                    if (parentNodeName === "STYLE") {
                      message =
                        `Lit does not support binding inside style nodes. ` +
                        `This is a security risk, as style injection attacks can ` +
                        `exfiltrate data and spoof UIs. ` +
                        `Consider instead using css\`...\` literals ` +
                        `to compose styles, and make do dynamic styling with ` +
                        `css custom properties, ::parts, <slot>s, ` +
                        `and by mutating the DOM rather than stylesheets.`;
                    } else {
                      message =
                        `Lit does not support binding inside script nodes. ` +
                        `This is a security risk, as it could allow arbitrary ` +
                        `code execution.`;
                    }
                  }
                  throw new Error(message);
                }
              }
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "commit node",
                    start: this._$startNode,
                    parent: this._$parent,
                    value: value,
                    options: this.options,
                  });
              this._$committedValue = this._insert(value);
            }
          }
          _commitText(value) {
            // If the committed value is a primitive it means we called _commitText on
            // the previous render, and we know that this._$startNode.nextSibling is a
            // Text node. We can now just replace the text content (.data) of the node.
            if (
              this._$committedValue !== nothing &&
              isPrimitive(this._$committedValue)
            ) {
              const node = wrap(this._$startNode).nextSibling;
              if (ENABLE_EXTRA_SECURITY_HOOKS) {
                if (this._textSanitizer === undefined) {
                  this._textSanitizer = createSanitizer(
                    node,
                    "data",
                    "property"
                  );
                }
                value = this._textSanitizer(value);
              }
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "commit text",
                    node,
                    value,
                    options: this.options,
                  });
              node.data = value;
            } else {
              if (ENABLE_EXTRA_SECURITY_HOOKS) {
                const textNode = document.createTextNode("");
                this._commitNode(textNode);
                // When setting text content, for security purposes it matters a lot
                // what the parent is. For example, <style> and <script> need to be
                // handled with care, while <span> does not. So first we need to put a
                // text node into the document, then we can sanitize its content.
                if (this._textSanitizer === undefined) {
                  this._textSanitizer = createSanitizer(
                    textNode,
                    "data",
                    "property"
                  );
                }
                value = this._textSanitizer(value);
                debugLogEvent === null || debugLogEvent === void 0
                  ? void 0
                  : debugLogEvent({
                      kind: "commit text",
                      node: textNode,
                      value,
                      options: this.options,
                    });
                textNode.data = value;
              } else {
                this._commitNode(d.createTextNode(value));
                debugLogEvent === null || debugLogEvent === void 0
                  ? void 0
                  : debugLogEvent({
                      kind: "commit text",
                      node: wrap(this._$startNode).nextSibling,
                      value,
                      options: this.options,
                    });
              }
            }
            this._$committedValue = value;
          }
          _commitTemplateResult(result) {
            var _a;
            // This property needs to remain unminified.
            const { values, ["_$litType$"]: type } = result;
            // If $litType$ is a number, result is a plain TemplateResult and we get
            // the template from the template cache. If not, result is a
            // CompiledTemplateResult and _$litType$ is a CompiledTemplate and we need
            // to create the <template> element the first time we see it.
            const template =
              typeof type === "number"
                ? this._$getTemplate(result)
                : (type.el === undefined &&
                    (type.el = Template.createElement(type.h, this.options)),
                  type);
            if (
              ((_a = this._$committedValue) === null || _a === void 0
                ? void 0
                : _a._$template) === template
            ) {
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "template updating",
                    template,
                    instance: this._$committedValue,
                    parts: this._$committedValue._parts,
                    options: this.options,
                    values,
                  });
              this._$committedValue._update(values);
            } else {
              const instance = new TemplateInstance(template, this);
              const fragment = instance._clone(this.options);
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "template instantiated",
                    template,
                    instance,
                    parts: instance._parts,
                    options: this.options,
                    fragment,
                    values,
                  });
              instance._update(values);
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "template instantiated and updated",
                    template,
                    instance,
                    parts: instance._parts,
                    options: this.options,
                    fragment,
                    values,
                  });
              this._commitNode(fragment);
              this._$committedValue = instance;
            }
          }
          // Overridden via `litHtmlPolyfillSupport` to provide platform support.
          /** @internal */
          _$getTemplate(result) {
            let template = templateCache.get(result.strings);
            if (template === undefined) {
              templateCache.set(
                result.strings,
                (template = new Template(result))
              );
            }
            return template;
          }
          _commitIterable(value) {
            // For an Iterable, we create a new InstancePart per item, then set its
            // value to the item. This is a little bit of overhead for every item in
            // an Iterable, but it lets us recurse easily and efficiently update Arrays
            // of TemplateResults that will be commonly returned from expressions like:
            // array.map((i) => html`${i}`), by reusing existing TemplateInstances.
            // If value is an array, then the previous render was of an
            // iterable and value will contain the ChildParts from the previous
            // render. If value is not an array, clear this part and make a new
            // array for ChildParts.
            if (!isArray(this._$committedValue)) {
              this._$committedValue = [];
              this._$clear();
            }
            // Lets us keep track of how many items we stamped so we can clear leftover
            // items from a previous render
            const itemParts = this._$committedValue;
            let partIndex = 0;
            let itemPart;
            for (const item of value) {
              if (partIndex === itemParts.length) {
                // If no existing part, create a new one
                // TODO (justinfagnani): test perf impact of always creating two parts
                // instead of sharing parts between nodes
                // https://github.com/lit/lit/issues/1266
                itemParts.push(
                  (itemPart = new ChildPart(
                    this._insert(createMarker()),
                    this._insert(createMarker()),
                    this,
                    this.options
                  ))
                );
              } else {
                // Reuse an existing part
                itemPart = itemParts[partIndex];
              }
              itemPart._$setValue(item);
              partIndex++;
            }
            if (partIndex < itemParts.length) {
              // itemParts always have end nodes
              this._$clear(
                itemPart && wrap(itemPart._$endNode).nextSibling,
                partIndex
              );
              // Truncate the parts array so _value reflects the current state
              itemParts.length = partIndex;
            }
          }
          /**
           * Removes the nodes contained within this Part from the DOM.
           *
           * @param start Start node to clear from, for clearing a subset of the part's
           *     DOM (used when truncating iterables)
           * @param from  When `start` is specified, the index within the iterable from
           *     which ChildParts are being removed, used for disconnecting directives in
           *     those Parts.
           *
           * @internal
           */
          _$clear(start = wrap(this._$startNode).nextSibling, from) {
            var _a;
            (_a = this._$notifyConnectionChanged) === null || _a === void 0
              ? void 0
              : _a.call(this, false, true, from);
            while (start && start !== this._$endNode) {
              const n = wrap(start).nextSibling;
              wrap(start).remove();
              start = n;
            }
          }
          /**
           * Implementation of RootPart's `isConnected`. Note that this metod
           * should only be called on `RootPart`s (the `ChildPart` returned from a
           * top-level `render()` call). It has no effect on non-root ChildParts.
           * @param isConnected Whether to set
           * @internal
           */
          setConnected(isConnected) {
            var _a;
            if (this._$parent === undefined) {
              this.__isConnected = isConnected;
              (_a = this._$notifyConnectionChanged) === null || _a === void 0
                ? void 0
                : _a.call(this, isConnected);
            } else if (DEV_MODE) {
              throw new Error(
                "part.setConnected() may only be called on a " +
                  "RootPart returned from render()."
              );
            }
          }
        }
        class AttributePart {
          constructor(element, name, strings, parent, options) {
            this.type = ATTRIBUTE_PART;
            /** @internal */
            this._$committedValue = nothing;
            /** @internal */
            this._$disconnectableChildren = undefined;
            this.element = element;
            this.name = name;
            this._$parent = parent;
            this.options = options;
            if (strings.length > 2 || strings[0] !== "" || strings[1] !== "") {
              this._$committedValue = new Array(strings.length - 1).fill(
                new String()
              );
              this.strings = strings;
            } else {
              this._$committedValue = nothing;
            }
            if (ENABLE_EXTRA_SECURITY_HOOKS) {
              this._sanitizer = undefined;
            }
          }
          get tagName() {
            return this.element.tagName;
          }
          // See comment in Disconnectable interface for why this is a getter
          get _$isConnected() {
            return this._$parent._$isConnected;
          }
          /**
           * Sets the value of this part by resolving the value from possibly multiple
           * values and static strings and committing it to the DOM.
           * If this part is single-valued, `this._strings` will be undefined, and the
           * method will be called with a single value argument. If this part is
           * multi-value, `this._strings` will be defined, and the method is called
           * with the value array of the part's owning TemplateInstance, and an offset
           * into the value array from which the values should be read.
           * This method is overloaded this way to eliminate short-lived array slices
           * of the template instance values, and allow a fast-path for single-valued
           * parts.
           *
           * @param value The part value, or an array of values for multi-valued parts
           * @param valueIndex the index to start reading values from. `undefined` for
           *   single-valued parts
           * @param noCommit causes the part to not commit its value to the DOM. Used
           *   in hydration to prime attribute parts with their first-rendered value,
           *   but not set the attribute, and in SSR to no-op the DOM operation and
           *   capture the value for serialization.
           *
           * @internal
           */
          _$setValue(value, directiveParent = this, valueIndex, noCommit) {
            const strings = this.strings;
            // Whether any of the values has changed, for dirty-checking
            let change = false;
            if (strings === undefined) {
              // Single-value binding case
              value = resolveDirective(this, value, directiveParent, 0);
              change =
                !isPrimitive(value) ||
                (value !== this._$committedValue && value !== noChange);
              if (change) {
                this._$committedValue = value;
              }
            } else {
              // Interpolation case
              const values = value;
              value = strings[0];
              let i, v;
              for (i = 0; i < strings.length - 1; i++) {
                v = resolveDirective(
                  this,
                  values[valueIndex + i],
                  directiveParent,
                  i
                );
                if (v === noChange) {
                  // If the user-provided value is `noChange`, use the previous value
                  v = this._$committedValue[i];
                }
                change ||
                  (change = !isPrimitive(v) || v !== this._$committedValue[i]);
                if (v === nothing) {
                  value = nothing;
                } else if (value !== nothing) {
                  value +=
                    (v !== null && v !== void 0 ? v : "") + strings[i + 1];
                }
                // We always record each value, even if one is `nothing`, for future
                // change detection.
                this._$committedValue[i] = v;
              }
            }
            if (change && !noCommit) {
              this._commitValue(value);
            }
          }
          /** @internal */
          _commitValue(value) {
            if (value === nothing) {
              wrap(this.element).removeAttribute(this.name);
            } else {
              if (ENABLE_EXTRA_SECURITY_HOOKS) {
                if (this._sanitizer === undefined) {
                  this._sanitizer = sanitizerFactoryInternal(
                    this.element,
                    this.name,
                    "attribute"
                  );
                }
                value = this._sanitizer(
                  value !== null && value !== void 0 ? value : ""
                );
              }
              debugLogEvent === null || debugLogEvent === void 0
                ? void 0
                : debugLogEvent({
                    kind: "commit attribute",
                    element: this.element,
                    name: this.name,
                    value,
                    options: this.options,
                  });
              wrap(this.element).setAttribute(
                this.name,
                value !== null && value !== void 0 ? value : ""
              );
            }
          }
        }
        class PropertyPart extends AttributePart {
          constructor() {
            super(...arguments);
            this.type = PROPERTY_PART;
          }
          /** @internal */
          _commitValue(value) {
            if (ENABLE_EXTRA_SECURITY_HOOKS) {
              if (this._sanitizer === undefined) {
                this._sanitizer = sanitizerFactoryInternal(
                  this.element,
                  this.name,
                  "property"
                );
              }
              value = this._sanitizer(value);
            }
            debugLogEvent === null || debugLogEvent === void 0
              ? void 0
              : debugLogEvent({
                  kind: "commit property",
                  element: this.element,
                  name: this.name,
                  value,
                  options: this.options,
                });
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            this.element[this.name] = value === nothing ? undefined : value;
          }
        }
        // Temporary workaround for https://crbug.com/993268
        // Currently, any attribute starting with "on" is considered to be a
        // TrustedScript source. Such boolean attributes must be set to the equivalent
        // trusted emptyScript value.
        const emptyStringForBooleanAttribute = trustedTypes
          ? trustedTypes.emptyScript
          : "";
        class BooleanAttributePart extends AttributePart {
          constructor() {
            super(...arguments);
            this.type = BOOLEAN_ATTRIBUTE_PART;
          }
          /** @internal */
          _commitValue(value) {
            debugLogEvent === null || debugLogEvent === void 0
              ? void 0
              : debugLogEvent({
                  kind: "commit boolean attribute",
                  element: this.element,
                  name: this.name,
                  value: !!(value && value !== nothing),
                  options: this.options,
                });
            if (value && value !== nothing) {
              wrap(this.element).setAttribute(
                this.name,
                emptyStringForBooleanAttribute
              );
            } else {
              wrap(this.element).removeAttribute(this.name);
            }
          }
        }
        class EventPart extends AttributePart {
          constructor(element, name, strings, parent, options) {
            super(element, name, strings, parent, options);
            this.type = EVENT_PART;
            if (DEV_MODE && this.strings !== undefined) {
              throw new Error(
                `A \`<${element.localName}>\` has a \`@${name}=...\` listener with ` +
                  "invalid content. Event listeners in templates must have exactly " +
                  "one expression and no surrounding text."
              );
            }
          }
          // EventPart does not use the base _$setValue/_resolveValue implementation
          // since the dirty checking is more complex
          /** @internal */
          _$setValue(newListener, directiveParent = this) {
            var _a;
            newListener =
              (_a = resolveDirective(this, newListener, directiveParent, 0)) !==
                null && _a !== void 0
                ? _a
                : nothing;
            if (newListener === noChange) {
              return;
            }
            const oldListener = this._$committedValue;
            // If the new value is nothing or any options change we have to remove the
            // part as a listener.
            const shouldRemoveListener =
              (newListener === nothing && oldListener !== nothing) ||
              newListener.capture !== oldListener.capture ||
              newListener.once !== oldListener.once ||
              newListener.passive !== oldListener.passive;
            // If the new value is not nothing and we removed the listener, we have
            // to add the part as a listener.
            const shouldAddListener =
              newListener !== nothing &&
              (oldListener === nothing || shouldRemoveListener);
            debugLogEvent === null || debugLogEvent === void 0
              ? void 0
              : debugLogEvent({
                  kind: "commit event listener",
                  element: this.element,
                  name: this.name,
                  value: newListener,
                  options: this.options,
                  removeListener: shouldRemoveListener,
                  addListener: shouldAddListener,
                  oldListener,
                });
            if (shouldRemoveListener) {
              this.element.removeEventListener(this.name, this, oldListener);
            }
            if (shouldAddListener) {
              // Beware: IE11 and Chrome 41 don't like using the listener as the
              // options object. Figure out how to deal w/ this in IE11 - maybe
              // patch addEventListener?
              this.element.addEventListener(this.name, this, newListener);
            }
            this._$committedValue = newListener;
          }
          handleEvent(event) {
            var _a, _b;
            if (typeof this._$committedValue === "function") {
              this._$committedValue.call(
                (_b =
                  (_a = this.options) === null || _a === void 0
                    ? void 0
                    : _a.host) !== null && _b !== void 0
                  ? _b
                  : this.element,
                event
              );
            } else {
              this._$committedValue.handleEvent(event);
            }
          }
        }
        class ElementPart {
          constructor(element, parent, options) {
            this.element = element;
            this.type = ELEMENT_PART;
            /** @internal */
            this._$disconnectableChildren = undefined;
            this._$parent = parent;
            this.options = options;
          }
          // See comment in Disconnectable interface for why this is a getter
          get _$isConnected() {
            return this._$parent._$isConnected;
          }
          _$setValue(value) {
            debugLogEvent === null || debugLogEvent === void 0
              ? void 0
              : debugLogEvent({
                  kind: "commit to element binding",
                  element: this.element,
                  value,
                  options: this.options,
                });
            resolveDirective(this, value);
          }
        }
        /**
         * END USERS SHOULD NOT RELY ON THIS OBJECT.
         *
         * Private exports for use by other Lit packages, not intended for use by
         * external users.
         *
         * We currently do not make a mangled rollup build of the lit-ssr code. In order
         * to keep a number of (otherwise private) top-level exports  mangled in the
         * client side code, we export a _$LH object containing those members (or
         * helper methods for accessing private fields of those members), and then
         * re-export them for use in lit-ssr. This keeps lit-ssr agnostic to whether the
         * client-side code is being used in `dev` mode or `prod` mode.
         *
         * This has a unique name, to disambiguate it from private exports in
         * lit-element, which re-exports all of lit-html.
         *
         * @private
         */
        const _$LH = {
          // Used in lit-ssr
          _boundAttributeSuffix: boundAttributeSuffix,
          _marker: marker,
          _markerMatch: markerMatch,
          _HTML_RESULT: HTML_RESULT,
          _getTemplateHtml: getTemplateHtml,
          // Used in hydrate
          _TemplateInstance: TemplateInstance,
          _isIterable: isIterable,
          _resolveDirective: resolveDirective,
          // Used in tests and private-ssr-support
          _ChildPart: ChildPart,
          _AttributePart: AttributePart,
          _BooleanAttributePart: BooleanAttributePart,
          _EventPart: EventPart,
          _PropertyPart: PropertyPart,
          _ElementPart: ElementPart,
        };
        // Apply polyfills if available
        const polyfillSupport = DEV_MODE
          ? global.litHtmlPolyfillSupportDevMode
          : global.litHtmlPolyfillSupport;
        polyfillSupport === null || polyfillSupport === void 0
          ? void 0
          : polyfillSupport(Template, ChildPart);
        // IMPORTANT: do not change the property name or the assignment expression.
        // This line will be used in regexes to search for lit-html usage.
        ((_d = global.litHtmlVersions) !== null && _d !== void 0
          ? _d
          : (global.litHtmlVersions = [])
        ).push("2.6.1");
        if (DEV_MODE && global.litHtmlVersions.length > 1) {
          issueWarning(
            "multiple-versions",
            `Multiple versions of Lit loaded. ` +
              `Loading multiple versions is not recommended.`
          );
        }
        /**
         * Renders a value, usually a lit-html TemplateResult, to the container.
         *
         * This example renders the text "Hello, Zoe!" inside a paragraph tag, appending
         * it to the container `document.body`.
         *
         * ```js
         * import {html, render} from 'lit';
         *
         * const name = "Zoe";
         * render(html`<p>Hello, ${name}!</p>`, document.body);
         * ```
         *
         * @param value Any [renderable
         *   value](https://lit.dev/docs/templates/expressions/#child-expressions),
         *   typically a {@linkcode TemplateResult} created by evaluating a template tag
         *   like {@linkcode html} or {@linkcode svg}.
         * @param container A DOM container to render to. The first render will append
         *   the rendered value to the container, and subsequent renders will
         *   efficiently update the rendered value if the same result type was
         *   previously rendered there.
         * @param options See {@linkcode RenderOptions} for options documentation.
         * @see
         * {@link https://lit.dev/docs/libraries/standalone-templates/#rendering-lit-html-templates| Rendering Lit HTML Templates}
         */
        const render = (value, container, options) => {
          var _a, _b;
          if (DEV_MODE && container == null) {
            // Give a clearer error message than
            //     Uncaught TypeError: Cannot read properties of null (reading
            //     '_$litPart$')
            // which reads like an internal Lit error.
            throw new TypeError(
              `The container to render into may not be ${container}`
            );
          }
          const renderId = DEV_MODE ? debugLogRenderId++ : 0;
          const partOwnerNode =
            (_a =
              options === null || options === void 0
                ? void 0
                : options.renderBefore) !== null && _a !== void 0
              ? _a
              : container;
          // This property needs to remain unminified.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          let part = partOwnerNode["_$litPart$"];
          debugLogEvent === null || debugLogEvent === void 0
            ? void 0
            : debugLogEvent({
                kind: "begin render",
                id: renderId,
                value,
                container,
                options,
                part,
              });
          if (part === undefined) {
            const endNode =
              (_b =
                options === null || options === void 0
                  ? void 0
                  : options.renderBefore) !== null && _b !== void 0
                ? _b
                : null;
            // This property needs to remain unminified.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            partOwnerNode["_$litPart$"] = part = new ChildPart(
              container.insertBefore(createMarker(), endNode),
              endNode,
              undefined,
              options !== null && options !== void 0 ? options : {}
            );
          }
          part._$setValue(value);
          debugLogEvent === null || debugLogEvent === void 0
            ? void 0
            : debugLogEvent({
                kind: "end render",
                id: renderId,
                value,
                container,
                options,
                part,
              });
          return part;
        };
        if (ENABLE_EXTRA_SECURITY_HOOKS) {
          render.setSanitizer = setSanitizer;
          render.createSanitizer = createSanitizer;
          if (DEV_MODE) {
            render._testOnlyClearSanitizerFactoryDoNotCallOrElse =
              _testOnlyClearSanitizerFactoryDoNotCallOrElse;
          }
        }
        //# sourceMappingURL=lit-html.js.map

        /***/
      },

    /******/
  };
  /************************************************************************/
  /******/ // The module cache
  /******/ var __webpack_module_cache__ = {};
  /******/
  /******/ // The require function
  /******/ function __webpack_require__(moduleId) {
    /******/ // Check if module is in cache
    /******/ var cachedModule = __webpack_module_cache__[moduleId];
    /******/ if (cachedModule !== undefined) {
      /******/ return cachedModule.exports;
      /******/
    }
    /******/ // Create a new module (and put it into the cache)
    /******/ var module = (__webpack_module_cache__[moduleId] = {
      /******/ // no module.id needed
      /******/ // no module.loaded needed
      /******/ exports: {},
      /******/
    });
    /******/
    /******/ // Execute the module function
    /******/ __webpack_modules__[moduleId](
      module,
      module.exports,
      __webpack_require__
    );
    /******/
    /******/ // Return the exports of the module
    /******/ return module.exports;
    /******/
  }
  /******/
  /************************************************************************/
  /******/ /* webpack/runtime/define property getters */
  /******/ (() => {
    /******/ // define getter functions for harmony exports
    /******/ __webpack_require__.d = (exports, definition) => {
      /******/ for (var key in definition) {
        /******/ if (
          __webpack_require__.o(definition, key) &&
          !__webpack_require__.o(exports, key)
        ) {
          /******/ Object.defineProperty(exports, key, {
            enumerable: true,
            get: definition[key],
          });
          /******/
        }
        /******/
      }
      /******/
    };
    /******/
  })();
  /******/
  /******/ /* webpack/runtime/hasOwnProperty shorthand */
  /******/ (() => {
    /******/ __webpack_require__.o = (obj, prop) =>
      Object.prototype.hasOwnProperty.call(obj, prop);
    /******/
  })();
  /******/
  /******/ /* webpack/runtime/make namespace object */
  /******/ (() => {
    /******/ // define __esModule on exports
    /******/ __webpack_require__.r = (exports) => {
      /******/ if (typeof Symbol !== "undefined" && Symbol.toStringTag) {
        /******/ Object.defineProperty(exports, Symbol.toStringTag, {
          value: "Module",
        });
        /******/
      }
      /******/ Object.defineProperty(exports, "__esModule", { value: true });
      /******/
    };
    /******/
  })();
  /******/
  /************************************************************************/
  var __webpack_exports__ = {};
  // This entry need to be wrapped in an IIFE because it need to be isolated against other modules in the chunk.
  (() => {
    /*!**********************!*\
  !*** ./src/index.ts ***!
  \**********************/
    __webpack_require__.r(__webpack_exports__);
    /* harmony import */ var _components_app_component__WEBPACK_IMPORTED_MODULE_0__ =
      __webpack_require__(
        /*! ./components/app-component */ "./src/components/app-component.ts"
      );

    const title = document.querySelector("title");
    title.textContent = "User Table";
    const body = document.querySelector("body");
    const appComponent = document.createElement("app-component");
    body.appendChild(appComponent);
  })();

  /******/
})();
//# sourceMappingURL=bundle-1fbf211b12ff415c1586.js.map
