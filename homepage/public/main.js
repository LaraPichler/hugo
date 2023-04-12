(() => {
  // <stdin>
  (() => {
    "use strict";
    var __webpack_modules__ = {
      /***/
      "./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js": (
        /*!*****************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js ***!
          \*****************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "BehaviorSubject": () => (
              /* binding */
              BehaviorSubject
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var _Subject__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./Subject */
            "./node_modules/rxjs/dist/esm5/internal/Subject.js"
          );
          var BehaviorSubject = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(BehaviorSubject2, _super);
            function BehaviorSubject2(_value) {
              var _this = _super.call(this) || this;
              _this._value = _value;
              return _this;
            }
            Object.defineProperty(BehaviorSubject2.prototype, "value", {
              get: function() {
                return this.getValue();
              },
              enumerable: false,
              configurable: true
            });
            BehaviorSubject2.prototype._subscribe = function(subscriber) {
              var subscription = _super.prototype._subscribe.call(this, subscriber);
              !subscription.closed && subscriber.next(this._value);
              return subscription;
            };
            BehaviorSubject2.prototype.getValue = function() {
              var _a = this, hasError = _a.hasError, thrownError = _a.thrownError, _value = _a._value;
              if (hasError) {
                throw thrownError;
              }
              this._throwIfClosed();
              return _value;
            };
            BehaviorSubject2.prototype.next = function(value) {
              _super.prototype.next.call(this, this._value = value);
            };
            return BehaviorSubject2;
          }(_Subject__WEBPACK_IMPORTED_MODULE_1__.Subject);
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js": (
        /*!***********************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js ***!
          \***********************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "COMPLETE_NOTIFICATION": () => (
              /* binding */
              COMPLETE_NOTIFICATION
            ),
            /* harmony export */
            "createNotification": () => (
              /* binding */
              createNotification
            ),
            /* harmony export */
            "errorNotification": () => (
              /* binding */
              errorNotification
            ),
            /* harmony export */
            "nextNotification": () => (
              /* binding */
              nextNotification
            )
            /* harmony export */
          });
          var COMPLETE_NOTIFICATION = function() {
            return createNotification("C", void 0, void 0);
          }();
          function errorNotification(error) {
            return createNotification("E", void 0, error);
          }
          function nextNotification(value) {
            return createNotification("N", value, void 0);
          }
          function createNotification(kind, value, error) {
            return {
              kind,
              value,
              error
            };
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/Observable.js": (
        /*!************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/Observable.js ***!
          \************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "Observable": () => (
              /* binding */
              Observable
            )
            /* harmony export */
          });
          var _Subscriber__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./Subscriber */
            "./node_modules/rxjs/dist/esm5/internal/Subscriber.js"
          );
          var _Subscription__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__2(
            /*! ./Subscription */
            "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
          var _symbol_observable__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ./symbol/observable */
            "./node_modules/rxjs/dist/esm5/internal/symbol/observable.js"
          );
          var _util_pipe__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__2(
            /*! ./util/pipe */
            "./node_modules/rxjs/dist/esm5/internal/util/pipe.js"
          );
          var _config__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__2(
            /*! ./config */
            "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
          var _util_isFunction__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__2(
            /*! ./util/isFunction */
            "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
          var _util_errorContext__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./util/errorContext */
            "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );
          var Observable = function() {
            function Observable2(subscribe) {
              if (subscribe) {
                this._subscribe = subscribe;
              }
            }
            Observable2.prototype.lift = function(operator) {
              var observable = new Observable2();
              observable.source = this;
              observable.operator = operator;
              return observable;
            };
            Observable2.prototype.subscribe = function(observerOrNext, error, complete) {
              var _this = this;
              var subscriber = isSubscriber(observerOrNext) ? observerOrNext : new _Subscriber__WEBPACK_IMPORTED_MODULE_0__.SafeSubscriber(observerOrNext, error, complete);
              (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_1__.errorContext)(function() {
                var _a = _this, operator = _a.operator, source = _a.source;
                subscriber.add(operator ? operator.call(subscriber, source) : source ? _this._subscribe(subscriber) : _this._trySubscribe(subscriber));
              });
              return subscriber;
            };
            Observable2.prototype._trySubscribe = function(sink) {
              try {
                return this._subscribe(sink);
              } catch (err) {
                sink.error(err);
              }
            };
            Observable2.prototype.forEach = function(next, promiseCtor) {
              var _this = this;
              promiseCtor = getPromiseCtor(promiseCtor);
              return new promiseCtor(function(resolve, reject) {
                var subscriber = new _Subscriber__WEBPACK_IMPORTED_MODULE_0__.SafeSubscriber({
                  next: function(value) {
                    try {
                      next(value);
                    } catch (err) {
                      reject(err);
                      subscriber.unsubscribe();
                    }
                  },
                  error: reject,
                  complete: resolve
                });
                _this.subscribe(subscriber);
              });
            };
            Observable2.prototype._subscribe = function(subscriber) {
              var _a;
              return (_a = this.source) === null || _a === void 0 ? void 0 : _a.subscribe(subscriber);
            };
            Observable2.prototype[_symbol_observable__WEBPACK_IMPORTED_MODULE_2__.observable] = function() {
              return this;
            };
            Observable2.prototype.pipe = function() {
              var operations = [];
              for (var _i = 0; _i < arguments.length; _i++) {
                operations[_i] = arguments[_i];
              }
              return (0, _util_pipe__WEBPACK_IMPORTED_MODULE_3__.pipeFromArray)(operations)(this);
            };
            Observable2.prototype.toPromise = function(promiseCtor) {
              var _this = this;
              promiseCtor = getPromiseCtor(promiseCtor);
              return new promiseCtor(function(resolve, reject) {
                var value;
                _this.subscribe(function(x) {
                  return value = x;
                }, function(err) {
                  return reject(err);
                }, function() {
                  return resolve(value);
                });
              });
            };
            Observable2.create = function(subscribe) {
              return new Observable2(subscribe);
            };
            return Observable2;
          }();
          function getPromiseCtor(promiseCtor) {
            var _a;
            return (_a = promiseCtor !== null && promiseCtor !== void 0 ? promiseCtor : _config__WEBPACK_IMPORTED_MODULE_4__.config.Promise) !== null && _a !== void 0 ? _a : Promise;
          }
          function isObserver(value) {
            return value && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(value.next) && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(value.error) && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_5__.isFunction)(value.complete);
          }
          function isSubscriber(value) {
            return value && value instanceof _Subscriber__WEBPACK_IMPORTED_MODULE_0__.Subscriber || isObserver(value) && (0, _Subscription__WEBPACK_IMPORTED_MODULE_6__.isSubscription)(value);
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/Subject.js": (
        /*!*********************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/Subject.js ***!
          \*********************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "AnonymousSubject": () => (
              /* binding */
              AnonymousSubject
            ),
            /* harmony export */
            "Subject": () => (
              /* binding */
              Subject
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var _Observable__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__2(
            /*! ./Observable */
            "./node_modules/rxjs/dist/esm5/internal/Observable.js"
          );
          var _Subscription__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__2(
            /*! ./Subscription */
            "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
          var _util_ObjectUnsubscribedError__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./util/ObjectUnsubscribedError */
            "./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js"
          );
          var _util_arrRemove__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__2(
            /*! ./util/arrRemove */
            "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js"
          );
          var _util_errorContext__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ./util/errorContext */
            "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );
          var Subject = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(Subject2, _super);
            function Subject2() {
              var _this = _super.call(this) || this;
              _this.closed = false;
              _this.currentObservers = null;
              _this.observers = [];
              _this.isStopped = false;
              _this.hasError = false;
              _this.thrownError = null;
              return _this;
            }
            Subject2.prototype.lift = function(operator) {
              var subject = new AnonymousSubject(this, this);
              subject.operator = operator;
              return subject;
            };
            Subject2.prototype._throwIfClosed = function() {
              if (this.closed) {
                throw new _util_ObjectUnsubscribedError__WEBPACK_IMPORTED_MODULE_1__.ObjectUnsubscribedError();
              }
            };
            Subject2.prototype.next = function(value) {
              var _this = this;
              (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(function() {
                var e_1, _a;
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  if (!_this.currentObservers) {
                    _this.currentObservers = Array.from(_this.observers);
                  }
                  try {
                    for (var _b = (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(_this.currentObservers), _c = _b.next(); !_c.done; _c = _b.next()) {
                      var observer = _c.value;
                      observer.next(value);
                    }
                  } catch (e_1_1) {
                    e_1 = { error: e_1_1 };
                  } finally {
                    try {
                      if (_c && !_c.done && (_a = _b.return))
                        _a.call(_b);
                    } finally {
                      if (e_1)
                        throw e_1.error;
                    }
                  }
                }
              });
            };
            Subject2.prototype.error = function(err) {
              var _this = this;
              (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(function() {
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  _this.hasError = _this.isStopped = true;
                  _this.thrownError = err;
                  var observers = _this.observers;
                  while (observers.length) {
                    observers.shift().error(err);
                  }
                }
              });
            };
            Subject2.prototype.complete = function() {
              var _this = this;
              (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_2__.errorContext)(function() {
                _this._throwIfClosed();
                if (!_this.isStopped) {
                  _this.isStopped = true;
                  var observers = _this.observers;
                  while (observers.length) {
                    observers.shift().complete();
                  }
                }
              });
            };
            Subject2.prototype.unsubscribe = function() {
              this.isStopped = this.closed = true;
              this.observers = this.currentObservers = null;
            };
            Object.defineProperty(Subject2.prototype, "observed", {
              get: function() {
                var _a;
                return ((_a = this.observers) === null || _a === void 0 ? void 0 : _a.length) > 0;
              },
              enumerable: false,
              configurable: true
            });
            Subject2.prototype._trySubscribe = function(subscriber) {
              this._throwIfClosed();
              return _super.prototype._trySubscribe.call(this, subscriber);
            };
            Subject2.prototype._subscribe = function(subscriber) {
              this._throwIfClosed();
              this._checkFinalizedStatuses(subscriber);
              return this._innerSubscribe(subscriber);
            };
            Subject2.prototype._innerSubscribe = function(subscriber) {
              var _this = this;
              var _a = this, hasError = _a.hasError, isStopped = _a.isStopped, observers = _a.observers;
              if (hasError || isStopped) {
                return _Subscription__WEBPACK_IMPORTED_MODULE_3__.EMPTY_SUBSCRIPTION;
              }
              this.currentObservers = null;
              observers.push(subscriber);
              return new _Subscription__WEBPACK_IMPORTED_MODULE_3__.Subscription(function() {
                _this.currentObservers = null;
                (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_4__.arrRemove)(observers, subscriber);
              });
            };
            Subject2.prototype._checkFinalizedStatuses = function(subscriber) {
              var _a = this, hasError = _a.hasError, thrownError = _a.thrownError, isStopped = _a.isStopped;
              if (hasError) {
                subscriber.error(thrownError);
              } else if (isStopped) {
                subscriber.complete();
              }
            };
            Subject2.prototype.asObservable = function() {
              var observable = new _Observable__WEBPACK_IMPORTED_MODULE_5__.Observable();
              observable.source = this;
              return observable;
            };
            Subject2.create = function(destination, source) {
              return new AnonymousSubject(destination, source);
            };
            return Subject2;
          }(_Observable__WEBPACK_IMPORTED_MODULE_5__.Observable);
          var AnonymousSubject = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(AnonymousSubject2, _super);
            function AnonymousSubject2(destination, source) {
              var _this = _super.call(this) || this;
              _this.destination = destination;
              _this.source = source;
              return _this;
            }
            AnonymousSubject2.prototype.next = function(value) {
              var _a, _b;
              (_b = (_a = this.destination) === null || _a === void 0 ? void 0 : _a.next) === null || _b === void 0 ? void 0 : _b.call(_a, value);
            };
            AnonymousSubject2.prototype.error = function(err) {
              var _a, _b;
              (_b = (_a = this.destination) === null || _a === void 0 ? void 0 : _a.error) === null || _b === void 0 ? void 0 : _b.call(_a, err);
            };
            AnonymousSubject2.prototype.complete = function() {
              var _a, _b;
              (_b = (_a = this.destination) === null || _a === void 0 ? void 0 : _a.complete) === null || _b === void 0 ? void 0 : _b.call(_a);
            };
            AnonymousSubject2.prototype._subscribe = function(subscriber) {
              var _a, _b;
              return (_b = (_a = this.source) === null || _a === void 0 ? void 0 : _a.subscribe(subscriber)) !== null && _b !== void 0 ? _b : _Subscription__WEBPACK_IMPORTED_MODULE_3__.EMPTY_SUBSCRIPTION;
            };
            return AnonymousSubject2;
          }(Subject);
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/Subscriber.js": (
        /*!************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/Subscriber.js ***!
          \************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "EMPTY_OBSERVER": () => (
              /* binding */
              EMPTY_OBSERVER
            ),
            /* harmony export */
            "SafeSubscriber": () => (
              /* binding */
              SafeSubscriber
            ),
            /* harmony export */
            "Subscriber": () => (
              /* binding */
              Subscriber
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var _util_isFunction__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__2(
            /*! ./util/isFunction */
            "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
          var _Subscription__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./Subscription */
            "./node_modules/rxjs/dist/esm5/internal/Subscription.js"
          );
          var _config__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__2(
            /*! ./config */
            "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
          var _util_reportUnhandledError__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__2(
            /*! ./util/reportUnhandledError */
            "./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js"
          );
          var _util_noop__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__2(
            /*! ./util/noop */
            "./node_modules/rxjs/dist/esm5/internal/util/noop.js"
          );
          var _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ./NotificationFactories */
            "./node_modules/rxjs/dist/esm5/internal/NotificationFactories.js"
          );
          var _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__2(
            /*! ./scheduler/timeoutProvider */
            "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js"
          );
          var _util_errorContext__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__2(
            /*! ./util/errorContext */
            "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js"
          );
          var Subscriber = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(Subscriber2, _super);
            function Subscriber2(destination) {
              var _this = _super.call(this) || this;
              _this.isStopped = false;
              if (destination) {
                _this.destination = destination;
                if ((0, _Subscription__WEBPACK_IMPORTED_MODULE_1__.isSubscription)(destination)) {
                  destination.add(_this);
                }
              } else {
                _this.destination = EMPTY_OBSERVER;
              }
              return _this;
            }
            Subscriber2.create = function(next, error, complete) {
              return new SafeSubscriber(next, error, complete);
            };
            Subscriber2.prototype.next = function(value) {
              if (this.isStopped) {
                handleStoppedNotification((0, _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.nextNotification)(value), this);
              } else {
                this._next(value);
              }
            };
            Subscriber2.prototype.error = function(err) {
              if (this.isStopped) {
                handleStoppedNotification((0, _NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.errorNotification)(err), this);
              } else {
                this.isStopped = true;
                this._error(err);
              }
            };
            Subscriber2.prototype.complete = function() {
              if (this.isStopped) {
                handleStoppedNotification(_NotificationFactories__WEBPACK_IMPORTED_MODULE_2__.COMPLETE_NOTIFICATION, this);
              } else {
                this.isStopped = true;
                this._complete();
              }
            };
            Subscriber2.prototype.unsubscribe = function() {
              if (!this.closed) {
                this.isStopped = true;
                _super.prototype.unsubscribe.call(this);
                this.destination = null;
              }
            };
            Subscriber2.prototype._next = function(value) {
              this.destination.next(value);
            };
            Subscriber2.prototype._error = function(err) {
              try {
                this.destination.error(err);
              } finally {
                this.unsubscribe();
              }
            };
            Subscriber2.prototype._complete = function() {
              try {
                this.destination.complete();
              } finally {
                this.unsubscribe();
              }
            };
            return Subscriber2;
          }(_Subscription__WEBPACK_IMPORTED_MODULE_1__.Subscription);
          var _bind = Function.prototype.bind;
          function bind(fn, thisArg) {
            return _bind.call(fn, thisArg);
          }
          var ConsumerObserver = function() {
            function ConsumerObserver2(partialObserver) {
              this.partialObserver = partialObserver;
            }
            ConsumerObserver2.prototype.next = function(value) {
              var partialObserver = this.partialObserver;
              if (partialObserver.next) {
                try {
                  partialObserver.next(value);
                } catch (error) {
                  handleUnhandledError(error);
                }
              }
            };
            ConsumerObserver2.prototype.error = function(err) {
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
            ConsumerObserver2.prototype.complete = function() {
              var partialObserver = this.partialObserver;
              if (partialObserver.complete) {
                try {
                  partialObserver.complete();
                } catch (error) {
                  handleUnhandledError(error);
                }
              }
            };
            return ConsumerObserver2;
          }();
          var SafeSubscriber = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(SafeSubscriber2, _super);
            function SafeSubscriber2(observerOrNext, error, complete) {
              var _this = _super.call(this) || this;
              var partialObserver;
              if ((0, _util_isFunction__WEBPACK_IMPORTED_MODULE_3__.isFunction)(observerOrNext) || !observerOrNext) {
                partialObserver = {
                  next: observerOrNext !== null && observerOrNext !== void 0 ? observerOrNext : void 0,
                  error: error !== null && error !== void 0 ? error : void 0,
                  complete: complete !== null && complete !== void 0 ? complete : void 0
                };
              } else {
                var context_1;
                if (_this && _config__WEBPACK_IMPORTED_MODULE_4__.config.useDeprecatedNextContext) {
                  context_1 = Object.create(observerOrNext);
                  context_1.unsubscribe = function() {
                    return _this.unsubscribe();
                  };
                  partialObserver = {
                    next: observerOrNext.next && bind(observerOrNext.next, context_1),
                    error: observerOrNext.error && bind(observerOrNext.error, context_1),
                    complete: observerOrNext.complete && bind(observerOrNext.complete, context_1)
                  };
                } else {
                  partialObserver = observerOrNext;
                }
              }
              _this.destination = new ConsumerObserver(partialObserver);
              return _this;
            }
            return SafeSubscriber2;
          }(Subscriber);
          function handleUnhandledError(error) {
            if (_config__WEBPACK_IMPORTED_MODULE_4__.config.useDeprecatedSynchronousErrorHandling) {
              (0, _util_errorContext__WEBPACK_IMPORTED_MODULE_5__.captureError)(error);
            } else {
              (0, _util_reportUnhandledError__WEBPACK_IMPORTED_MODULE_6__.reportUnhandledError)(error);
            }
          }
          function defaultErrorHandler(err) {
            throw err;
          }
          function handleStoppedNotification(notification, subscriber) {
            var onStoppedNotification = _config__WEBPACK_IMPORTED_MODULE_4__.config.onStoppedNotification;
            onStoppedNotification && _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_7__.timeoutProvider.setTimeout(function() {
              return onStoppedNotification(notification, subscriber);
            });
          }
          var EMPTY_OBSERVER = {
            closed: true,
            next: _util_noop__WEBPACK_IMPORTED_MODULE_8__.noop,
            error: defaultErrorHandler,
            complete: _util_noop__WEBPACK_IMPORTED_MODULE_8__.noop
          };
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/Subscription.js": (
        /*!**************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/Subscription.js ***!
          \**************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "EMPTY_SUBSCRIPTION": () => (
              /* binding */
              EMPTY_SUBSCRIPTION
            ),
            /* harmony export */
            "Subscription": () => (
              /* binding */
              Subscription
            ),
            /* harmony export */
            "isSubscription": () => (
              /* binding */
              isSubscription
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var _util_isFunction__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./util/isFunction */
            "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
          var _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ./util/UnsubscriptionError */
            "./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js"
          );
          var _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__2(
            /*! ./util/arrRemove */
            "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js"
          );
          var Subscription = function() {
            function Subscription2(initialTeardown) {
              this.initialTeardown = initialTeardown;
              this.closed = false;
              this._parentage = null;
              this._finalizers = null;
            }
            Subscription2.prototype.unsubscribe = function() {
              var e_1, _a, e_2, _b;
              var errors;
              if (!this.closed) {
                this.closed = true;
                var _parentage = this._parentage;
                if (_parentage) {
                  this._parentage = null;
                  if (Array.isArray(_parentage)) {
                    try {
                      for (var _parentage_1 = (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(_parentage), _parentage_1_1 = _parentage_1.next(); !_parentage_1_1.done; _parentage_1_1 = _parentage_1.next()) {
                        var parent_1 = _parentage_1_1.value;
                        parent_1.remove(this);
                      }
                    } catch (e_1_1) {
                      e_1 = { error: e_1_1 };
                    } finally {
                      try {
                        if (_parentage_1_1 && !_parentage_1_1.done && (_a = _parentage_1.return))
                          _a.call(_parentage_1);
                      } finally {
                        if (e_1)
                          throw e_1.error;
                      }
                    }
                  } else {
                    _parentage.remove(this);
                  }
                }
                var initialFinalizer = this.initialTeardown;
                if ((0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(initialFinalizer)) {
                  try {
                    initialFinalizer();
                  } catch (e) {
                    errors = e instanceof _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError ? e.errors : [e];
                  }
                }
                var _finalizers = this._finalizers;
                if (_finalizers) {
                  this._finalizers = null;
                  try {
                    for (var _finalizers_1 = (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__values)(_finalizers), _finalizers_1_1 = _finalizers_1.next(); !_finalizers_1_1.done; _finalizers_1_1 = _finalizers_1.next()) {
                      var finalizer = _finalizers_1_1.value;
                      try {
                        execFinalizer(finalizer);
                      } catch (err) {
                        errors = errors !== null && errors !== void 0 ? errors : [];
                        if (err instanceof _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError) {
                          errors = (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)((0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)([], (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(errors)), (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(err.errors));
                        } else {
                          errors.push(err);
                        }
                      }
                    }
                  } catch (e_2_1) {
                    e_2 = { error: e_2_1 };
                  } finally {
                    try {
                      if (_finalizers_1_1 && !_finalizers_1_1.done && (_b = _finalizers_1.return))
                        _b.call(_finalizers_1);
                    } finally {
                      if (e_2)
                        throw e_2.error;
                    }
                  }
                }
                if (errors) {
                  throw new _util_UnsubscriptionError__WEBPACK_IMPORTED_MODULE_2__.UnsubscriptionError(errors);
                }
              }
            };
            Subscription2.prototype.add = function(teardown) {
              var _a;
              if (teardown && teardown !== this) {
                if (this.closed) {
                  execFinalizer(teardown);
                } else {
                  if (teardown instanceof Subscription2) {
                    if (teardown.closed || teardown._hasParent(this)) {
                      return;
                    }
                    teardown._addParent(this);
                  }
                  (this._finalizers = (_a = this._finalizers) !== null && _a !== void 0 ? _a : []).push(teardown);
                }
              }
            };
            Subscription2.prototype._hasParent = function(parent) {
              var _parentage = this._parentage;
              return _parentage === parent || Array.isArray(_parentage) && _parentage.includes(parent);
            };
            Subscription2.prototype._addParent = function(parent) {
              var _parentage = this._parentage;
              this._parentage = Array.isArray(_parentage) ? (_parentage.push(parent), _parentage) : _parentage ? [_parentage, parent] : parent;
            };
            Subscription2.prototype._removeParent = function(parent) {
              var _parentage = this._parentage;
              if (_parentage === parent) {
                this._parentage = null;
              } else if (Array.isArray(_parentage)) {
                (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__.arrRemove)(_parentage, parent);
              }
            };
            Subscription2.prototype.remove = function(teardown) {
              var _finalizers = this._finalizers;
              _finalizers && (0, _util_arrRemove__WEBPACK_IMPORTED_MODULE_3__.arrRemove)(_finalizers, teardown);
              if (teardown instanceof Subscription2) {
                teardown._removeParent(this);
              }
            };
            Subscription2.EMPTY = function() {
              var empty = new Subscription2();
              empty.closed = true;
              return empty;
            }();
            return Subscription2;
          }();
          var EMPTY_SUBSCRIPTION = Subscription.EMPTY;
          function isSubscription(value) {
            return value instanceof Subscription || value && "closed" in value && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(value.remove) && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(value.add) && (0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(value.unsubscribe);
          }
          function execFinalizer(finalizer) {
            if ((0, _util_isFunction__WEBPACK_IMPORTED_MODULE_1__.isFunction)(finalizer)) {
              finalizer();
            } else {
              finalizer.unsubscribe();
            }
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/config.js": (
        /*!********************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/config.js ***!
          \********************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "config": () => (
              /* binding */
              config
            )
            /* harmony export */
          });
          var config = {
            onUnhandledError: null,
            onStoppedNotification: null,
            Promise: void 0,
            useDeprecatedSynchronousErrorHandling: false,
            useDeprecatedNextContext: false
          };
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js": (
        /*!******************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js ***!
          \******************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "OperatorSubscriber": () => (
              /* binding */
              OperatorSubscriber
            ),
            /* harmony export */
            "createOperatorSubscriber": () => (
              /* binding */
              createOperatorSubscriber
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var _Subscriber__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ../Subscriber */
            "./node_modules/rxjs/dist/esm5/internal/Subscriber.js"
          );
          function createOperatorSubscriber(destination, onNext, onComplete, onError, onFinalize) {
            return new OperatorSubscriber(destination, onNext, onComplete, onError, onFinalize);
          }
          var OperatorSubscriber = function(_super) {
            (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__extends)(OperatorSubscriber2, _super);
            function OperatorSubscriber2(destination, onNext, onComplete, onError, onFinalize, shouldUnsubscribe) {
              var _this = _super.call(this, destination) || this;
              _this.onFinalize = onFinalize;
              _this.shouldUnsubscribe = shouldUnsubscribe;
              _this._next = onNext ? function(value) {
                try {
                  onNext(value);
                } catch (err) {
                  destination.error(err);
                }
              } : _super.prototype._next;
              _this._error = onError ? function(err) {
                try {
                  onError(err);
                } catch (err2) {
                  destination.error(err2);
                } finally {
                  this.unsubscribe();
                }
              } : _super.prototype._error;
              _this._complete = onComplete ? function() {
                try {
                  onComplete();
                } catch (err) {
                  destination.error(err);
                } finally {
                  this.unsubscribe();
                }
              } : _super.prototype._complete;
              return _this;
            }
            OperatorSubscriber2.prototype.unsubscribe = function() {
              var _a;
              if (!this.shouldUnsubscribe || this.shouldUnsubscribe()) {
                var closed_1 = this.closed;
                _super.prototype.unsubscribe.call(this);
                !closed_1 && ((_a = this.onFinalize) === null || _a === void 0 ? void 0 : _a.call(this));
              }
            };
            return OperatorSubscriber2;
          }(_Subscriber__WEBPACK_IMPORTED_MODULE_1__.Subscriber);
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js": (
        /*!********************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js ***!
          \********************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "distinctUntilChanged": () => (
              /* binding */
              distinctUntilChanged
            )
            /* harmony export */
          });
          var _util_identity__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ../util/identity */
            "./node_modules/rxjs/dist/esm5/internal/util/identity.js"
          );
          var _util_lift__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ../util/lift */
            "./node_modules/rxjs/dist/esm5/internal/util/lift.js"
          );
          var _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ./OperatorSubscriber */
            "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js"
          );
          function distinctUntilChanged(comparator, keySelector) {
            if (keySelector === void 0) {
              keySelector = _util_identity__WEBPACK_IMPORTED_MODULE_0__.identity;
            }
            comparator = comparator !== null && comparator !== void 0 ? comparator : defaultCompare;
            return (0, _util_lift__WEBPACK_IMPORTED_MODULE_1__.operate)(function(source, subscriber) {
              var previousKey;
              var first = true;
              source.subscribe((0, _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_2__.createOperatorSubscriber)(subscriber, function(value) {
                var currentKey = keySelector(value);
                if (first || !comparator(previousKey, currentKey)) {
                  first = false;
                  previousKey = currentKey;
                  subscriber.next(value);
                }
              }));
            });
          }
          function defaultCompare(a, b) {
            return a === b;
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/operators/map.js": (
        /*!***************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/operators/map.js ***!
          \***************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "map": () => (
              /* binding */
              map
            )
            /* harmony export */
          });
          var _util_lift__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ../util/lift */
            "./node_modules/rxjs/dist/esm5/internal/util/lift.js"
          );
          var _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./OperatorSubscriber */
            "./node_modules/rxjs/dist/esm5/internal/operators/OperatorSubscriber.js"
          );
          function map(project, thisArg) {
            return (0, _util_lift__WEBPACK_IMPORTED_MODULE_0__.operate)(function(source, subscriber) {
              var index = 0;
              source.subscribe((0, _OperatorSubscriber__WEBPACK_IMPORTED_MODULE_1__.createOperatorSubscriber)(subscriber, function(value) {
                subscriber.next(project.call(thisArg, value, index++));
              }));
            });
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js": (
        /*!***************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js ***!
          \***************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "timeoutProvider": () => (
              /* binding */
              timeoutProvider
            )
            /* harmony export */
          });
          var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! tslib */
            "./node_modules/tslib/tslib.es6.js"
          );
          var timeoutProvider = {
            setTimeout: function(handler, timeout) {
              var args = [];
              for (var _i = 2; _i < arguments.length; _i++) {
                args[_i - 2] = arguments[_i];
              }
              var delegate = timeoutProvider.delegate;
              if (delegate === null || delegate === void 0 ? void 0 : delegate.setTimeout) {
                return delegate.setTimeout.apply(delegate, (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)([handler, timeout], (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(args)));
              }
              return setTimeout.apply(void 0, (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__spreadArray)([handler, timeout], (0, tslib__WEBPACK_IMPORTED_MODULE_0__.__read)(args)));
            },
            clearTimeout: function(handle) {
              var delegate = timeoutProvider.delegate;
              return ((delegate === null || delegate === void 0 ? void 0 : delegate.clearTimeout) || clearTimeout)(handle);
            },
            delegate: void 0
          };
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/symbol/observable.js": (
        /*!*******************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/symbol/observable.js ***!
          \*******************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "observable": () => (
              /* binding */
              observable
            )
            /* harmony export */
          });
          var observable = function() {
            return typeof Symbol === "function" && Symbol.observable || "@@observable";
          }();
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js": (
        /*!******************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/ObjectUnsubscribedError.js ***!
          \******************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "ObjectUnsubscribedError": () => (
              /* binding */
              ObjectUnsubscribedError
            )
            /* harmony export */
          });
          var _createErrorClass__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./createErrorClass */
            "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js"
          );
          var ObjectUnsubscribedError = (0, _createErrorClass__WEBPACK_IMPORTED_MODULE_0__.createErrorClass)(function(_super) {
            return function ObjectUnsubscribedErrorImpl() {
              _super(this);
              this.name = "ObjectUnsubscribedError";
              this.message = "object unsubscribed";
            };
          });
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js": (
        /*!**************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/UnsubscriptionError.js ***!
          \**************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "UnsubscriptionError": () => (
              /* binding */
              UnsubscriptionError
            )
            /* harmony export */
          });
          var _createErrorClass__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./createErrorClass */
            "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js"
          );
          var UnsubscriptionError = (0, _createErrorClass__WEBPACK_IMPORTED_MODULE_0__.createErrorClass)(function(_super) {
            return function UnsubscriptionErrorImpl(errors) {
              _super(this);
              this.message = errors ? errors.length + " errors occurred during unsubscription:\n" + errors.map(function(err, i) {
                return i + 1 + ") " + err.toString();
              }).join("\n  ") : "";
              this.name = "UnsubscriptionError";
              this.errors = errors;
            };
          });
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js": (
        /*!****************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/arrRemove.js ***!
          \****************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "arrRemove": () => (
              /* binding */
              arrRemove
            )
            /* harmony export */
          });
          function arrRemove(arr, item) {
            if (arr) {
              var index = arr.indexOf(item);
              0 <= index && arr.splice(index, 1);
            }
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js": (
        /*!***********************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/createErrorClass.js ***!
          \***********************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "createErrorClass": () => (
              /* binding */
              createErrorClass
            )
            /* harmony export */
          });
          function createErrorClass(createImpl) {
            var _super = function(instance) {
              Error.call(instance);
              instance.stack = new Error().stack;
            };
            var ctorFunc = createImpl(_super);
            ctorFunc.prototype = Object.create(Error.prototype);
            ctorFunc.prototype.constructor = ctorFunc;
            return ctorFunc;
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/errorContext.js": (
        /*!*******************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/errorContext.js ***!
          \*******************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "captureError": () => (
              /* binding */
              captureError
            ),
            /* harmony export */
            "errorContext": () => (
              /* binding */
              errorContext
            )
            /* harmony export */
          });
          var _config__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ../config */
            "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
          var context = null;
          function errorContext(cb) {
            if (_config__WEBPACK_IMPORTED_MODULE_0__.config.useDeprecatedSynchronousErrorHandling) {
              var isRoot = !context;
              if (isRoot) {
                context = { errorThrown: false, error: null };
              }
              cb();
              if (isRoot) {
                var _a = context, errorThrown = _a.errorThrown, error = _a.error;
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
            if (_config__WEBPACK_IMPORTED_MODULE_0__.config.useDeprecatedSynchronousErrorHandling && context) {
              context.errorThrown = true;
              context.error = err;
            }
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/identity.js": (
        /*!***************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/identity.js ***!
          \***************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "identity": () => (
              /* binding */
              identity
            )
            /* harmony export */
          });
          function identity(x) {
            return x;
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js": (
        /*!*****************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/isFunction.js ***!
          \*****************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "isFunction": () => (
              /* binding */
              isFunction
            )
            /* harmony export */
          });
          function isFunction(value) {
            return typeof value === "function";
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/lift.js": (
        /*!***********************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/lift.js ***!
          \***********************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "hasLift": () => (
              /* binding */
              hasLift
            ),
            /* harmony export */
            "operate": () => (
              /* binding */
              operate
            )
            /* harmony export */
          });
          var _isFunction__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./isFunction */
            "./node_modules/rxjs/dist/esm5/internal/util/isFunction.js"
          );
          function hasLift(source) {
            return (0, _isFunction__WEBPACK_IMPORTED_MODULE_0__.isFunction)(source === null || source === void 0 ? void 0 : source.lift);
          }
          function operate(init) {
            return function(source) {
              if (hasLift(source)) {
                return source.lift(function(liftedSource) {
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
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/noop.js": (
        /*!***********************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/noop.js ***!
          \***********************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "noop": () => (
              /* binding */
              noop
            )
            /* harmony export */
          });
          function noop() {
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/pipe.js": (
        /*!***********************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/pipe.js ***!
          \***********************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "pipe": () => (
              /* binding */
              pipe
            ),
            /* harmony export */
            "pipeFromArray": () => (
              /* binding */
              pipeFromArray
            )
            /* harmony export */
          });
          var _identity__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./identity */
            "./node_modules/rxjs/dist/esm5/internal/util/identity.js"
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
              return fns.reduce(function(prev, fn) {
                return fn(prev);
              }, input);
            };
          }
        }
      ),
      /***/
      "./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js": (
        /*!***************************************************************************!*\
          !*** ./node_modules/rxjs/dist/esm5/internal/util/reportUnhandledError.js ***!
          \***************************************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "reportUnhandledError": () => (
              /* binding */
              reportUnhandledError
            )
            /* harmony export */
          });
          var _config__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ../config */
            "./node_modules/rxjs/dist/esm5/internal/config.js"
          );
          var _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ../scheduler/timeoutProvider */
            "./node_modules/rxjs/dist/esm5/internal/scheduler/timeoutProvider.js"
          );
          function reportUnhandledError(err) {
            _scheduler_timeoutProvider__WEBPACK_IMPORTED_MODULE_0__.timeoutProvider.setTimeout(function() {
              var onUnhandledError = _config__WEBPACK_IMPORTED_MODULE_1__.config.onUnhandledError;
              if (onUnhandledError) {
                onUnhandledError(err);
              } else {
                throw err;
              }
            });
          }
        }
      ),
      /***/
      "./src/components/app-component.ts": (
        /*!*****************************************!*\
          !*** ./src/components/app-component.ts ***!
          \*****************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          var lit_html__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! lit-html */
            "./node_modules/lit-html/development/lit-html.js"
          );
          var _user__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./user */
            "./src/components/user/index.ts"
          );
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
              (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(template, this.shadowRoot);
              const userTableComponent = this.shadowRoot.querySelector("user-table");
              const userComponent = this.shadowRoot.querySelector("user-component");
              userTableComponent.addEventListener(_user__WEBPACK_IMPORTED_MODULE_1__.USER_SELECTED_EVENT, (e) => {
                const user = e.detail.user;
                userComponent.setAttribute("selected-user", user.id);
                userComponent.style.display = "block";
                userTableComponent.style.display = "none";
                console.log("event: user selected:", user);
              });
            }
          }
          customElements.define("app-component", AppComponent);
        }
      ),
      /***/
      "./src/components/user/index.ts": (
        /*!**************************************!*\
          !*** ./src/components/user/index.ts ***!
          \**************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "USER_SELECTED_EVENT": () => (
              /* binding */
              USER_SELECTED_EVENT
            )
            /* harmony export */
          });
          var _user_component__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./user-component */
            "./src/components/user/user-component.ts"
          );
          var _user_table_component__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! ./user-table-component */
            "./src/components/user/user-table-component.ts"
          );
          const USER_SELECTED_EVENT = "user-selected";
        }
      ),
      /***/
      "./src/components/user/user-component.ts": (
        /*!***********************************************!*\
          !*** ./src/components/user/user-component.ts ***!
          \***********************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          var lit_html__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! lit-html */
            "./node_modules/lit-html/development/lit-html.js"
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
              (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(template, this.shadowRoot);
            }
          }
          customElements.define("user-component", UserComponent);
        }
      ),
      /***/
      "./src/components/user/user-table-component.ts": (
        /*!*****************************************************!*\
          !*** ./src/components/user/user-table-component.ts ***!
          \*****************************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "UserTableComponent": () => (
              /* binding */
              UserTableComponent
            )
            /* harmony export */
          });
          var lit_html__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! lit-html */
            "./node_modules/lit-html/development/lit-html.js"
          );
          var ___WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! . */
            "./src/components/user/index.ts"
          );
          var _user_service__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__2(
            /*! ../../user-service */
            "./src/user-service.ts"
          );
          var _model_store__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__2(
            /*! ../../model/store */
            "./src/model/store.ts"
          );
          var rxjs__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__2(
            /*! rxjs */
            "./node_modules/rxjs/dist/esm5/internal/operators/map.js"
          );
          var rxjs__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__2(
            /*! rxjs */
            "./node_modules/rxjs/dist/esm5/internal/operators/distinctUntilChanged.js"
          );
          const rowTemplate = (user, onclick) => lit_html__WEBPACK_IMPORTED_MODULE_0__.html`
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
              _model_store__WEBPACK_IMPORTED_MODULE_3__["default"].pipe((0, rxjs__WEBPACK_IMPORTED_MODULE_4__.map)((model) => model.users), (0, rxjs__WEBPACK_IMPORTED_MODULE_5__.distinctUntilChanged)()).subscribe((users) => {
                this.render(users);
              });
            }
            render(users) {
              this._updateComplete = false;
              const userClicked = (user) => {
                alert(`user ${user.name} selected`);
                this.dispatchEvent(new CustomEvent(___WEBPACK_IMPORTED_MODULE_1__.USER_SELECTED_EVENT, { detail: { user } }));
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
              (0, lit_html__WEBPACK_IMPORTED_MODULE_0__.render)(tableTemplate, this.shadowRoot);
              this._updateComplete = true;
            }
          }
          customElements.define("user-table", UserTableComponent);
        }
      ),
      /***/
      "./src/model/store.ts": (
        /*!****************************!*\
          !*** ./src/model/store.ts ***!
          \****************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "default": () => __WEBPACK_DEFAULT_EXPORT__,
            /* harmony export */
            "setUsers": () => (
              /* binding */
              setUsers
            )
            /* harmony export */
          });
          var immer__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! immer */
            "./node_modules/immer/dist/immer.esm.mjs"
          );
          var rxjs__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__2(
            /*! rxjs */
            "./node_modules/rxjs/dist/esm5/internal/BehaviorSubject.js"
          );
          const store = createStore();
          function setUsers(users) {
            let nextState = (0, immer__WEBPACK_IMPORTED_MODULE_0__["default"])(store.getValue(), (draft) => {
              draft.users = users;
            });
            store.next(nextState);
          }
          function createStore() {
            const initialState = {
              users: []
            };
            return new rxjs__WEBPACK_IMPORTED_MODULE_1__.BehaviorSubject(initialState);
          }
          const __WEBPACK_DEFAULT_EXPORT__ = store;
        }
      ),
      /***/
      "./src/user-service.ts": (
        /*!*****************************!*\
          !*** ./src/user-service.ts ***!
          \*****************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "default": () => __WEBPACK_DEFAULT_EXPORT__
            /* harmony export */
          });
          var _model_store__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__2(
            /*! ./model/store */
            "./src/model/store.ts"
          );
          const USER_URL = "./api/user";
          class UserService {
            async fetchAll() {
              const response = await fetch(USER_URL);
              const users = await response.json();
              (0, _model_store__WEBPACK_IMPORTED_MODULE_0__.setUsers)(users);
            }
          }
          const userService = new UserService();
          const __WEBPACK_DEFAULT_EXPORT__ = userService;
        }
      ),
      /***/
      "./node_modules/tslib/tslib.es6.js": (
        /*!*****************************************!*\
          !*** ./node_modules/tslib/tslib.es6.js ***!
          \*****************************************/
        /***/
        (__unused_webpack_module, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "__assign": () => (
              /* binding */
              __assign
            ),
            /* harmony export */
            "__asyncDelegator": () => (
              /* binding */
              __asyncDelegator
            ),
            /* harmony export */
            "__asyncGenerator": () => (
              /* binding */
              __asyncGenerator
            ),
            /* harmony export */
            "__asyncValues": () => (
              /* binding */
              __asyncValues
            ),
            /* harmony export */
            "__await": () => (
              /* binding */
              __await
            ),
            /* harmony export */
            "__awaiter": () => (
              /* binding */
              __awaiter
            ),
            /* harmony export */
            "__classPrivateFieldGet": () => (
              /* binding */
              __classPrivateFieldGet
            ),
            /* harmony export */
            "__classPrivateFieldIn": () => (
              /* binding */
              __classPrivateFieldIn
            ),
            /* harmony export */
            "__classPrivateFieldSet": () => (
              /* binding */
              __classPrivateFieldSet
            ),
            /* harmony export */
            "__createBinding": () => (
              /* binding */
              __createBinding
            ),
            /* harmony export */
            "__decorate": () => (
              /* binding */
              __decorate
            ),
            /* harmony export */
            "__esDecorate": () => (
              /* binding */
              __esDecorate
            ),
            /* harmony export */
            "__exportStar": () => (
              /* binding */
              __exportStar
            ),
            /* harmony export */
            "__extends": () => (
              /* binding */
              __extends
            ),
            /* harmony export */
            "__generator": () => (
              /* binding */
              __generator
            ),
            /* harmony export */
            "__importDefault": () => (
              /* binding */
              __importDefault
            ),
            /* harmony export */
            "__importStar": () => (
              /* binding */
              __importStar
            ),
            /* harmony export */
            "__makeTemplateObject": () => (
              /* binding */
              __makeTemplateObject
            ),
            /* harmony export */
            "__metadata": () => (
              /* binding */
              __metadata
            ),
            /* harmony export */
            "__param": () => (
              /* binding */
              __param
            ),
            /* harmony export */
            "__propKey": () => (
              /* binding */
              __propKey
            ),
            /* harmony export */
            "__read": () => (
              /* binding */
              __read
            ),
            /* harmony export */
            "__rest": () => (
              /* binding */
              __rest
            ),
            /* harmony export */
            "__runInitializers": () => (
              /* binding */
              __runInitializers
            ),
            /* harmony export */
            "__setFunctionName": () => (
              /* binding */
              __setFunctionName
            ),
            /* harmony export */
            "__spread": () => (
              /* binding */
              __spread
            ),
            /* harmony export */
            "__spreadArray": () => (
              /* binding */
              __spreadArray
            ),
            /* harmony export */
            "__spreadArrays": () => (
              /* binding */
              __spreadArrays
            ),
            /* harmony export */
            "__values": () => (
              /* binding */
              __values
            )
            /* harmony export */
          });
          var extendStatics = function(d, b) {
            extendStatics = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(d2, b2) {
              d2.__proto__ = b2;
            } || function(d2, b2) {
              for (var p in b2)
                if (Object.prototype.hasOwnProperty.call(b2, p))
                  d2[p] = b2[p];
            };
            return extendStatics(d, b);
          };
          function __extends(d, b) {
            if (typeof b !== "function" && b !== null)
              throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
            extendStatics(d, b);
            function __() {
              this.constructor = d;
            }
            d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
          }
          var __assign = function() {
            __assign = Object.assign || function __assign2(t) {
              for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s)
                  if (Object.prototype.hasOwnProperty.call(s, p))
                    t[p] = s[p];
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
              for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
                if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                  t[p[i]] = s[p[i]];
              }
            return t;
          }
          function __decorate(decorators, target, key, desc) {
            var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
            if (typeof Reflect === "object" && typeof Reflect.decorate === "function")
              r = Reflect.decorate(decorators, target, key, desc);
            else
              for (var i = decorators.length - 1; i >= 0; i--)
                if (d = decorators[i])
                  r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
            return c > 3 && r && Object.defineProperty(target, key, r), r;
          }
          function __param(paramIndex, decorator) {
            return function(target, key) {
              decorator(target, key, paramIndex);
            };
          }
          function __esDecorate(ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
            function accept(f) {
              if (f !== void 0 && typeof f !== "function")
                throw new TypeError("Function expected");
              return f;
            }
            var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
            var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
            var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
            var _, done = false;
            for (var i = decorators.length - 1; i >= 0; i--) {
              var context = {};
              for (var p in contextIn)
                context[p] = p === "access" ? {} : contextIn[p];
              for (var p in contextIn.access)
                context.access[p] = contextIn.access[p];
              context.addInitializer = function(f) {
                if (done)
                  throw new TypeError("Cannot add initializers after decoration has completed");
                extraInitializers.push(accept(f || null));
              };
              var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
              if (kind === "accessor") {
                if (result === void 0)
                  continue;
                if (result === null || typeof result !== "object")
                  throw new TypeError("Object expected");
                if (_ = accept(result.get))
                  descriptor.get = _;
                if (_ = accept(result.set))
                  descriptor.set = _;
                if (_ = accept(result.init))
                  initializers.push(_);
              } else if (_ = accept(result)) {
                if (kind === "field")
                  initializers.push(_);
                else
                  descriptor[key] = _;
              }
            }
            if (target)
              Object.defineProperty(target, contextIn.name, descriptor);
            done = true;
          }
          ;
          function __runInitializers(thisArg, initializers, value) {
            var useValue = arguments.length > 2;
            for (var i = 0; i < initializers.length; i++) {
              value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
            }
            return useValue ? value : void 0;
          }
          ;
          function __propKey(x) {
            return typeof x === "symbol" ? x : "".concat(x);
          }
          ;
          function __setFunctionName(f, name, prefix) {
            if (typeof name === "symbol")
              name = name.description ? "[".concat(name.description, "]") : "";
            return Object.defineProperty(f, "name", { configurable: true, value: prefix ? "".concat(prefix, " ", name) : name });
          }
          ;
          function __metadata(metadataKey, metadataValue) {
            if (typeof Reflect === "object" && typeof Reflect.metadata === "function")
              return Reflect.metadata(metadataKey, metadataValue);
          }
          function __awaiter(thisArg, _arguments, P, generator) {
            function adopt(value) {
              return value instanceof P ? value : new P(function(resolve) {
                resolve(value);
              });
            }
            return new (P || (P = Promise))(function(resolve, reject) {
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
                result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected);
              }
              step((generator = generator.apply(thisArg, _arguments || [])).next());
            });
          }
          function __generator(thisArg, body) {
            var _ = { label: 0, sent: function() {
              if (t[0] & 1)
                throw t[1];
              return t[1];
            }, trys: [], ops: [] }, f, y, t, g;
            return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() {
              return this;
            }), g;
            function verb(n) {
              return function(v) {
                return step([n, v]);
              };
            }
            function step(op) {
              if (f)
                throw new TypeError("Generator is already executing.");
              while (g && (g = 0, op[0] && (_ = 0)), _)
                try {
                  if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done)
                    return t;
                  if (y = 0, t)
                    op = [op[0] & 2, t.value];
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
                      if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) {
                        _ = 0;
                        continue;
                      }
                      if (op[0] === 3 && (!t || op[1] > t[0] && op[1] < t[3])) {
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
                      if (t[2])
                        _.ops.pop();
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
              if (op[0] & 5)
                throw op[1];
              return { value: op[0] ? op[1] : void 0, done: true };
            }
          }
          var __createBinding = Object.create ? function(o, m, k, k2) {
            if (k2 === void 0)
              k2 = k;
            var desc = Object.getOwnPropertyDescriptor(m, k);
            if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
              desc = { enumerable: true, get: function() {
                return m[k];
              } };
            }
            Object.defineProperty(o, k2, desc);
          } : function(o, m, k, k2) {
            if (k2 === void 0)
              k2 = k;
            o[k2] = m[k];
          };
          function __exportStar(m, o) {
            for (var p in m)
              if (p !== "default" && !Object.prototype.hasOwnProperty.call(o, p))
                __createBinding(o, m, p);
          }
          function __values(o) {
            var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
            if (m)
              return m.call(o);
            if (o && typeof o.length === "number")
              return {
                next: function() {
                  if (o && i >= o.length)
                    o = void 0;
                  return { value: o && o[i++], done: !o };
                }
              };
            throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
          }
          function __read(o, n) {
            var m = typeof Symbol === "function" && o[Symbol.iterator];
            if (!m)
              return o;
            var i = m.call(o), r, ar = [], e;
            try {
              while ((n === void 0 || n-- > 0) && !(r = i.next()).done)
                ar.push(r.value);
            } catch (error) {
              e = { error };
            } finally {
              try {
                if (r && !r.done && (m = i["return"]))
                  m.call(i);
              } finally {
                if (e)
                  throw e.error;
              }
            }
            return ar;
          }
          function __spread() {
            for (var ar = [], i = 0; i < arguments.length; i++)
              ar = ar.concat(__read(arguments[i]));
            return ar;
          }
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
                  if (!ar)
                    ar = Array.prototype.slice.call(from, 0, i);
                  ar[i] = from[i];
                }
              }
            return to.concat(ar || Array.prototype.slice.call(from));
          }
          function __await(v) {
            return this instanceof __await ? (this.v = v, this) : new __await(v);
          }
          function __asyncGenerator(thisArg, _arguments, generator) {
            if (!Symbol.asyncIterator)
              throw new TypeError("Symbol.asyncIterator is not defined.");
            var g = generator.apply(thisArg, _arguments || []), i, q = [];
            return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function() {
              return this;
            }, i;
            function verb(n) {
              if (g[n])
                i[n] = function(v) {
                  return new Promise(function(a, b) {
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
              r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r);
            }
            function fulfill(value) {
              resume("next", value);
            }
            function reject(value) {
              resume("throw", value);
            }
            function settle(f, v) {
              if (f(v), q.shift(), q.length)
                resume(q[0][0], q[0][1]);
            }
          }
          function __asyncDelegator(o) {
            var i, p;
            return i = {}, verb("next"), verb("throw", function(e) {
              throw e;
            }), verb("return"), i[Symbol.iterator] = function() {
              return this;
            }, i;
            function verb(n, f) {
              i[n] = o[n] ? function(v) {
                return (p = !p) ? { value: __await(o[n](v)), done: false } : f ? f(v) : v;
              } : f;
            }
          }
          function __asyncValues(o) {
            if (!Symbol.asyncIterator)
              throw new TypeError("Symbol.asyncIterator is not defined.");
            var m = o[Symbol.asyncIterator], i;
            return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function() {
              return this;
            }, i);
            function verb(n) {
              i[n] = o[n] && function(v) {
                return new Promise(function(resolve, reject) {
                  v = o[n](v), settle(resolve, reject, v.done, v.value);
                });
              };
            }
            function settle(resolve, reject, d, v) {
              Promise.resolve(v).then(function(v2) {
                resolve({ value: v2, done: d });
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
          ;
          var __setModuleDefault = Object.create ? function(o, v) {
            Object.defineProperty(o, "default", { enumerable: true, value: v });
          } : function(o, v) {
            o["default"] = v;
          };
          function __importStar(mod) {
            if (mod && mod.__esModule)
              return mod;
            var result = {};
            if (mod != null) {
              for (var k in mod)
                if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k))
                  __createBinding(result, mod, k);
            }
            __setModuleDefault(result, mod);
            return result;
          }
          function __importDefault(mod) {
            return mod && mod.__esModule ? mod : { default: mod };
          }
          function __classPrivateFieldGet(receiver, state, kind, f) {
            if (kind === "a" && !f)
              throw new TypeError("Private accessor was defined without a getter");
            if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver))
              throw new TypeError("Cannot read private member from an object whose class did not declare it");
            return kind === "m" ? f : kind === "a" ? f.call(receiver) : f ? f.value : state.get(receiver);
          }
          function __classPrivateFieldSet(receiver, state, value, kind, f) {
            if (kind === "m")
              throw new TypeError("Private method is not writable");
            if (kind === "a" && !f)
              throw new TypeError("Private accessor was defined without a setter");
            if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver))
              throw new TypeError("Cannot write private member to an object whose class did not declare it");
            return kind === "a" ? f.call(receiver, value) : f ? f.value = value : state.set(receiver, value), value;
          }
          function __classPrivateFieldIn(state, receiver) {
            if (receiver === null || typeof receiver !== "object" && typeof receiver !== "function")
              throw new TypeError("Cannot use 'in' operator on non-object");
            return typeof state === "function" ? receiver === state : state.has(receiver);
          }
        }
      ),
      /***/
      "./node_modules/immer/dist/immer.esm.mjs": (
        /*!***********************************************!*\
          !*** ./node_modules/immer/dist/immer.esm.mjs ***!
          \***********************************************/
        /***/
        (__unused_webpack___webpack_module__, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "Immer": () => (
              /* binding */
              un
            ),
            /* harmony export */
            "applyPatches": () => (
              /* binding */
              pn
            ),
            /* harmony export */
            "castDraft": () => (
              /* binding */
              K
            ),
            /* harmony export */
            "castImmutable": () => (
              /* binding */
              $
            ),
            /* harmony export */
            "createDraft": () => (
              /* binding */
              ln
            ),
            /* harmony export */
            "current": () => (
              /* binding */
              R
            ),
            /* harmony export */
            "default": () => __WEBPACK_DEFAULT_EXPORT__,
            /* harmony export */
            "enableAllPlugins": () => (
              /* binding */
              J
            ),
            /* harmony export */
            "enableES5": () => (
              /* binding */
              F
            ),
            /* harmony export */
            "enableMapSet": () => (
              /* binding */
              C
            ),
            /* harmony export */
            "enablePatches": () => (
              /* binding */
              T
            ),
            /* harmony export */
            "finishDraft": () => (
              /* binding */
              dn
            ),
            /* harmony export */
            "freeze": () => (
              /* binding */
              d
            ),
            /* harmony export */
            "immerable": () => (
              /* binding */
              L
            ),
            /* harmony export */
            "isDraft": () => (
              /* binding */
              r
            ),
            /* harmony export */
            "isDraftable": () => (
              /* binding */
              t
            ),
            /* harmony export */
            "nothing": () => (
              /* binding */
              H
            ),
            /* harmony export */
            "original": () => (
              /* binding */
              e
            ),
            /* harmony export */
            "produce": () => (
              /* binding */
              fn
            ),
            /* harmony export */
            "produceWithPatches": () => (
              /* binding */
              cn
            ),
            /* harmony export */
            "setAutoFreeze": () => (
              /* binding */
              sn
            ),
            /* harmony export */
            "setUseProxies": () => (
              /* binding */
              vn
            )
            /* harmony export */
          });
          function n(n2) {
            for (var r2 = arguments.length, t2 = Array(r2 > 1 ? r2 - 1 : 0), e2 = 1; e2 < r2; e2++)
              t2[e2 - 1] = arguments[e2];
            if (true) {
              var i2 = Y[n2], o2 = i2 ? "function" == typeof i2 ? i2.apply(null, t2) : i2 : "unknown error nr: " + n2;
              throw Error("[Immer] " + o2);
            }
            throw Error("[Immer] minified error nr: " + n2 + (t2.length ? " " + t2.map(function(n3) {
              return "'" + n3 + "'";
            }).join(",") : "") + ". Find the full error at: https://bit.ly/3cXEKWf");
          }
          function r(n2) {
            return !!n2 && !!n2[Q];
          }
          function t(n2) {
            var r2;
            return !!n2 && (function(n3) {
              if (!n3 || "object" != typeof n3)
                return false;
              var r3 = Object.getPrototypeOf(n3);
              if (null === r3)
                return true;
              var t2 = Object.hasOwnProperty.call(r3, "constructor") && r3.constructor;
              return t2 === Object || "function" == typeof t2 && Function.toString.call(t2) === Z;
            }(n2) || Array.isArray(n2) || !!n2[L] || !!(null === (r2 = n2.constructor) || void 0 === r2 ? void 0 : r2[L]) || s(n2) || v(n2));
          }
          function e(t2) {
            return r(t2) || n(23, t2), t2[Q].t;
          }
          function i(n2, r2, t2) {
            void 0 === t2 && (t2 = false), 0 === o(n2) ? (t2 ? Object.keys : nn)(n2).forEach(function(e2) {
              t2 && "symbol" == typeof e2 || r2(e2, n2[e2], n2);
            }) : n2.forEach(function(t3, e2) {
              return r2(e2, t3, n2);
            });
          }
          function o(n2) {
            var r2 = n2[Q];
            return r2 ? r2.i > 3 ? r2.i - 4 : r2.i : Array.isArray(n2) ? 1 : s(n2) ? 2 : v(n2) ? 3 : 0;
          }
          function u(n2, r2) {
            return 2 === o(n2) ? n2.has(r2) : Object.prototype.hasOwnProperty.call(n2, r2);
          }
          function a(n2, r2) {
            return 2 === o(n2) ? n2.get(r2) : n2[r2];
          }
          function f(n2, r2, t2) {
            var e2 = o(n2);
            2 === e2 ? n2.set(r2, t2) : 3 === e2 ? n2.add(t2) : n2[r2] = t2;
          }
          function c(n2, r2) {
            return n2 === r2 ? 0 !== n2 || 1 / n2 == 1 / r2 : n2 != n2 && r2 != r2;
          }
          function s(n2) {
            return X && n2 instanceof Map;
          }
          function v(n2) {
            return q && n2 instanceof Set;
          }
          function p(n2) {
            return n2.o || n2.t;
          }
          function l(n2) {
            if (Array.isArray(n2))
              return Array.prototype.slice.call(n2);
            var r2 = rn(n2);
            delete r2[Q];
            for (var t2 = nn(r2), e2 = 0; e2 < t2.length; e2++) {
              var i2 = t2[e2], o2 = r2[i2];
              false === o2.writable && (o2.writable = true, o2.configurable = true), (o2.get || o2.set) && (r2[i2] = { configurable: true, writable: true, enumerable: o2.enumerable, value: n2[i2] });
            }
            return Object.create(Object.getPrototypeOf(n2), r2);
          }
          function d(n2, e2) {
            return void 0 === e2 && (e2 = false), y(n2) || r(n2) || !t(n2) || (o(n2) > 1 && (n2.set = n2.add = n2.clear = n2.delete = h), Object.freeze(n2), e2 && i(n2, function(n3, r2) {
              return d(r2, true);
            }, true)), n2;
          }
          function h() {
            n(2);
          }
          function y(n2) {
            return null == n2 || "object" != typeof n2 || Object.isFrozen(n2);
          }
          function b(r2) {
            var t2 = tn[r2];
            return t2 || n(18, r2), t2;
          }
          function m(n2, r2) {
            tn[n2] || (tn[n2] = r2);
          }
          function _() {
            return U || n(0), U;
          }
          function j(n2, r2) {
            r2 && (b("Patches"), n2.u = [], n2.s = [], n2.v = r2);
          }
          function O(n2) {
            g(n2), n2.p.forEach(S), n2.p = null;
          }
          function g(n2) {
            n2 === U && (U = n2.l);
          }
          function w(n2) {
            return U = { p: [], l: U, h: n2, m: true, _: 0 };
          }
          function S(n2) {
            var r2 = n2[Q];
            0 === r2.i || 1 === r2.i ? r2.j() : r2.O = true;
          }
          function P(r2, e2) {
            e2._ = e2.p.length;
            var i2 = e2.p[0], o2 = void 0 !== r2 && r2 !== i2;
            return e2.h.g || b("ES5").S(e2, r2, o2), o2 ? (i2[Q].P && (O(e2), n(4)), t(r2) && (r2 = M(e2, r2), e2.l || x(e2, r2)), e2.u && b("Patches").M(i2[Q].t, r2, e2.u, e2.s)) : r2 = M(e2, i2, []), O(e2), e2.u && e2.v(e2.u, e2.s), r2 !== H ? r2 : void 0;
          }
          function M(n2, r2, t2) {
            if (y(r2))
              return r2;
            var e2 = r2[Q];
            if (!e2)
              return i(r2, function(i2, o3) {
                return A(n2, e2, r2, i2, o3, t2);
              }, true), r2;
            if (e2.A !== n2)
              return r2;
            if (!e2.P)
              return x(n2, e2.t, true), e2.t;
            if (!e2.I) {
              e2.I = true, e2.A._--;
              var o2 = 4 === e2.i || 5 === e2.i ? e2.o = l(e2.k) : e2.o, u2 = o2, a2 = false;
              3 === e2.i && (u2 = new Set(o2), o2.clear(), a2 = true), i(u2, function(r3, i2) {
                return A(n2, e2, o2, r3, i2, t2, a2);
              }), x(n2, o2, false), t2 && n2.u && b("Patches").N(e2, t2, n2.u, n2.s);
            }
            return e2.o;
          }
          function A(e2, i2, o2, a2, c2, s2, v2) {
            if (c2 === o2 && n(5), r(c2)) {
              var p2 = M(e2, c2, s2 && i2 && 3 !== i2.i && !u(i2.R, a2) ? s2.concat(a2) : void 0);
              if (f(o2, a2, p2), !r(p2))
                return;
              e2.m = false;
            } else
              v2 && o2.add(c2);
            if (t(c2) && !y(c2)) {
              if (!e2.h.D && e2._ < 1)
                return;
              M(e2, c2), i2 && i2.A.l || x(e2, c2);
            }
          }
          function x(n2, r2, t2) {
            void 0 === t2 && (t2 = false), !n2.l && n2.h.D && n2.m && d(r2, t2);
          }
          function z(n2, r2) {
            var t2 = n2[Q];
            return (t2 ? p(t2) : n2)[r2];
          }
          function I(n2, r2) {
            if (r2 in n2)
              for (var t2 = Object.getPrototypeOf(n2); t2; ) {
                var e2 = Object.getOwnPropertyDescriptor(t2, r2);
                if (e2)
                  return e2;
                t2 = Object.getPrototypeOf(t2);
              }
          }
          function k(n2) {
            n2.P || (n2.P = true, n2.l && k(n2.l));
          }
          function E(n2) {
            n2.o || (n2.o = l(n2.t));
          }
          function N(n2, r2, t2) {
            var e2 = s(r2) ? b("MapSet").F(r2, t2) : v(r2) ? b("MapSet").T(r2, t2) : n2.g ? function(n3, r3) {
              var t3 = Array.isArray(n3), e3 = { i: t3 ? 1 : 0, A: r3 ? r3.A : _(), P: false, I: false, R: {}, l: r3, t: n3, k: null, o: null, j: null, C: false }, i2 = e3, o2 = en;
              t3 && (i2 = [e3], o2 = on);
              var u2 = Proxy.revocable(i2, o2), a2 = u2.revoke, f2 = u2.proxy;
              return e3.k = f2, e3.j = a2, f2;
            }(r2, t2) : b("ES5").J(r2, t2);
            return (t2 ? t2.A : _()).p.push(e2), e2;
          }
          function R(e2) {
            return r(e2) || n(22, e2), function n2(r2) {
              if (!t(r2))
                return r2;
              var e3, u2 = r2[Q], c2 = o(r2);
              if (u2) {
                if (!u2.P && (u2.i < 4 || !b("ES5").K(u2)))
                  return u2.t;
                u2.I = true, e3 = D(r2, c2), u2.I = false;
              } else
                e3 = D(r2, c2);
              return i(e3, function(r3, t2) {
                u2 && a(u2.t, r3) === t2 || f(e3, r3, n2(t2));
              }), 3 === c2 ? new Set(e3) : e3;
            }(e2);
          }
          function D(n2, r2) {
            switch (r2) {
              case 2:
                return new Map(n2);
              case 3:
                return Array.from(n2);
            }
            return l(n2);
          }
          function F() {
            function t2(n2, r2) {
              var t3 = s2[n2];
              return t3 ? t3.enumerable = r2 : s2[n2] = t3 = { configurable: true, enumerable: r2, get: function() {
                var r3 = this[Q];
                return f2(r3), en.get(r3, n2);
              }, set: function(r3) {
                var t4 = this[Q];
                f2(t4), en.set(t4, n2, r3);
              } }, t3;
            }
            function e2(n2) {
              for (var r2 = n2.length - 1; r2 >= 0; r2--) {
                var t3 = n2[r2][Q];
                if (!t3.P)
                  switch (t3.i) {
                    case 5:
                      a2(t3) && k(t3);
                      break;
                    case 4:
                      o2(t3) && k(t3);
                  }
              }
            }
            function o2(n2) {
              for (var r2 = n2.t, t3 = n2.k, e3 = nn(t3), i2 = e3.length - 1; i2 >= 0; i2--) {
                var o3 = e3[i2];
                if (o3 !== Q) {
                  var a3 = r2[o3];
                  if (void 0 === a3 && !u(r2, o3))
                    return true;
                  var f3 = t3[o3], s3 = f3 && f3[Q];
                  if (s3 ? s3.t !== a3 : !c(f3, a3))
                    return true;
                }
              }
              var v2 = !!r2[Q];
              return e3.length !== nn(r2).length + (v2 ? 0 : 1);
            }
            function a2(n2) {
              var r2 = n2.k;
              if (r2.length !== n2.t.length)
                return true;
              var t3 = Object.getOwnPropertyDescriptor(r2, r2.length - 1);
              if (t3 && !t3.get)
                return true;
              for (var e3 = 0; e3 < r2.length; e3++)
                if (!r2.hasOwnProperty(e3))
                  return true;
              return false;
            }
            function f2(r2) {
              r2.O && n(3, JSON.stringify(p(r2)));
            }
            var s2 = {};
            m("ES5", { J: function(n2, r2) {
              var e3 = Array.isArray(n2), i2 = function(n3, r3) {
                if (n3) {
                  for (var e4 = Array(r3.length), i3 = 0; i3 < r3.length; i3++)
                    Object.defineProperty(e4, "" + i3, t2(i3, true));
                  return e4;
                }
                var o4 = rn(r3);
                delete o4[Q];
                for (var u2 = nn(o4), a3 = 0; a3 < u2.length; a3++) {
                  var f3 = u2[a3];
                  o4[f3] = t2(f3, n3 || !!o4[f3].enumerable);
                }
                return Object.create(Object.getPrototypeOf(r3), o4);
              }(e3, n2), o3 = { i: e3 ? 5 : 4, A: r2 ? r2.A : _(), P: false, I: false, R: {}, l: r2, t: n2, k: i2, o: null, O: false, C: false };
              return Object.defineProperty(i2, Q, { value: o3, writable: true }), i2;
            }, S: function(n2, t3, o3) {
              o3 ? r(t3) && t3[Q].A === n2 && e2(n2.p) : (n2.u && function n3(r2) {
                if (r2 && "object" == typeof r2) {
                  var t4 = r2[Q];
                  if (t4) {
                    var e3 = t4.t, o4 = t4.k, f3 = t4.R, c2 = t4.i;
                    if (4 === c2)
                      i(o4, function(r3) {
                        r3 !== Q && (void 0 !== e3[r3] || u(e3, r3) ? f3[r3] || n3(o4[r3]) : (f3[r3] = true, k(t4)));
                      }), i(e3, function(n4) {
                        void 0 !== o4[n4] || u(o4, n4) || (f3[n4] = false, k(t4));
                      });
                    else if (5 === c2) {
                      if (a2(t4) && (k(t4), f3.length = true), o4.length < e3.length)
                        for (var s3 = o4.length; s3 < e3.length; s3++)
                          f3[s3] = false;
                      else
                        for (var v2 = e3.length; v2 < o4.length; v2++)
                          f3[v2] = true;
                      for (var p2 = Math.min(o4.length, e3.length), l2 = 0; l2 < p2; l2++)
                        o4.hasOwnProperty(l2) || (f3[l2] = true), void 0 === f3[l2] && n3(o4[l2]);
                    }
                  }
                }
              }(n2.p[0]), e2(n2.p));
            }, K: function(n2) {
              return 4 === n2.i ? o2(n2) : a2(n2);
            } });
          }
          function T() {
            function e2(n2) {
              if (!t(n2))
                return n2;
              if (Array.isArray(n2))
                return n2.map(e2);
              if (s(n2))
                return new Map(Array.from(n2.entries()).map(function(n3) {
                  return [n3[0], e2(n3[1])];
                }));
              if (v(n2))
                return new Set(Array.from(n2).map(e2));
              var r2 = Object.create(Object.getPrototypeOf(n2));
              for (var i2 in n2)
                r2[i2] = e2(n2[i2]);
              return u(n2, L) && (r2[L] = n2[L]), r2;
            }
            function f2(n2) {
              return r(n2) ? e2(n2) : n2;
            }
            var c2 = "add";
            m("Patches", { $: function(r2, t2) {
              return t2.forEach(function(t3) {
                for (var i2 = t3.path, u2 = t3.op, f3 = r2, s2 = 0; s2 < i2.length - 1; s2++) {
                  var v2 = o(f3), p2 = "" + i2[s2];
                  0 !== v2 && 1 !== v2 || "__proto__" !== p2 && "constructor" !== p2 || n(24), "function" == typeof f3 && "prototype" === p2 && n(24), "object" != typeof (f3 = a(f3, p2)) && n(15, i2.join("/"));
                }
                var l2 = o(f3), d2 = e2(t3.value), h2 = i2[i2.length - 1];
                switch (u2) {
                  case "replace":
                    switch (l2) {
                      case 2:
                        return f3.set(h2, d2);
                      case 3:
                        n(16);
                      default:
                        return f3[h2] = d2;
                    }
                  case c2:
                    switch (l2) {
                      case 1:
                        return "-" === h2 ? f3.push(d2) : f3.splice(h2, 0, d2);
                      case 2:
                        return f3.set(h2, d2);
                      case 3:
                        return f3.add(d2);
                      default:
                        return f3[h2] = d2;
                    }
                  case "remove":
                    switch (l2) {
                      case 1:
                        return f3.splice(h2, 1);
                      case 2:
                        return f3.delete(h2);
                      case 3:
                        return f3.delete(t3.value);
                      default:
                        return delete f3[h2];
                    }
                  default:
                    n(17, u2);
                }
              }), r2;
            }, N: function(n2, r2, t2, e3) {
              switch (n2.i) {
                case 0:
                case 4:
                case 2:
                  return function(n3, r3, t3, e4) {
                    var o2 = n3.t, s2 = n3.o;
                    i(n3.R, function(n4, i2) {
                      var v2 = a(o2, n4), p2 = a(s2, n4), l2 = i2 ? u(o2, n4) ? "replace" : c2 : "remove";
                      if (v2 !== p2 || "replace" !== l2) {
                        var d2 = r3.concat(n4);
                        t3.push("remove" === l2 ? { op: l2, path: d2 } : { op: l2, path: d2, value: p2 }), e4.push(l2 === c2 ? { op: "remove", path: d2 } : "remove" === l2 ? { op: c2, path: d2, value: f2(v2) } : { op: "replace", path: d2, value: f2(v2) });
                      }
                    });
                  }(n2, r2, t2, e3);
                case 5:
                case 1:
                  return function(n3, r3, t3, e4) {
                    var i2 = n3.t, o2 = n3.R, u2 = n3.o;
                    if (u2.length < i2.length) {
                      var a2 = [u2, i2];
                      i2 = a2[0], u2 = a2[1];
                      var s2 = [e4, t3];
                      t3 = s2[0], e4 = s2[1];
                    }
                    for (var v2 = 0; v2 < i2.length; v2++)
                      if (o2[v2] && u2[v2] !== i2[v2]) {
                        var p2 = r3.concat([v2]);
                        t3.push({ op: "replace", path: p2, value: f2(u2[v2]) }), e4.push({ op: "replace", path: p2, value: f2(i2[v2]) });
                      }
                    for (var l2 = i2.length; l2 < u2.length; l2++) {
                      var d2 = r3.concat([l2]);
                      t3.push({ op: c2, path: d2, value: f2(u2[l2]) });
                    }
                    i2.length < u2.length && e4.push({ op: "replace", path: r3.concat(["length"]), value: i2.length });
                  }(n2, r2, t2, e3);
                case 3:
                  return function(n3, r3, t3, e4) {
                    var i2 = n3.t, o2 = n3.o, u2 = 0;
                    i2.forEach(function(n4) {
                      if (!o2.has(n4)) {
                        var i3 = r3.concat([u2]);
                        t3.push({ op: "remove", path: i3, value: n4 }), e4.unshift({ op: c2, path: i3, value: n4 });
                      }
                      u2++;
                    }), u2 = 0, o2.forEach(function(n4) {
                      if (!i2.has(n4)) {
                        var o3 = r3.concat([u2]);
                        t3.push({ op: c2, path: o3, value: n4 }), e4.unshift({ op: "remove", path: o3, value: n4 });
                      }
                      u2++;
                    });
                  }(n2, r2, t2, e3);
              }
            }, M: function(n2, r2, t2, e3) {
              t2.push({ op: "replace", path: [], value: r2 === H ? void 0 : r2 }), e3.push({ op: "replace", path: [], value: n2 });
            } });
          }
          function C() {
            function r2(n2, r3) {
              function t2() {
                this.constructor = n2;
              }
              a2(n2, r3), n2.prototype = (t2.prototype = r3.prototype, new t2());
            }
            function e2(n2) {
              n2.o || (n2.R = /* @__PURE__ */ new Map(), n2.o = new Map(n2.t));
            }
            function o2(n2) {
              n2.o || (n2.o = /* @__PURE__ */ new Set(), n2.t.forEach(function(r3) {
                if (t(r3)) {
                  var e3 = N(n2.A.h, r3, n2);
                  n2.p.set(r3, e3), n2.o.add(e3);
                } else
                  n2.o.add(r3);
              }));
            }
            function u2(r3) {
              r3.O && n(3, JSON.stringify(p(r3)));
            }
            var a2 = function(n2, r3) {
              return (a2 = Object.setPrototypeOf || { __proto__: [] } instanceof Array && function(n3, r4) {
                n3.__proto__ = r4;
              } || function(n3, r4) {
                for (var t2 in r4)
                  r4.hasOwnProperty(t2) && (n3[t2] = r4[t2]);
              })(n2, r3);
            }, f2 = function() {
              function n2(n3, r3) {
                return this[Q] = { i: 2, l: r3, A: r3 ? r3.A : _(), P: false, I: false, o: void 0, R: void 0, t: n3, k: this, C: false, O: false }, this;
              }
              r2(n2, Map);
              var o3 = n2.prototype;
              return Object.defineProperty(o3, "size", { get: function() {
                return p(this[Q]).size;
              } }), o3.has = function(n3) {
                return p(this[Q]).has(n3);
              }, o3.set = function(n3, r3) {
                var t2 = this[Q];
                return u2(t2), p(t2).has(n3) && p(t2).get(n3) === r3 || (e2(t2), k(t2), t2.R.set(n3, true), t2.o.set(n3, r3), t2.R.set(n3, true)), this;
              }, o3.delete = function(n3) {
                if (!this.has(n3))
                  return false;
                var r3 = this[Q];
                return u2(r3), e2(r3), k(r3), r3.t.has(n3) ? r3.R.set(n3, false) : r3.R.delete(n3), r3.o.delete(n3), true;
              }, o3.clear = function() {
                var n3 = this[Q];
                u2(n3), p(n3).size && (e2(n3), k(n3), n3.R = /* @__PURE__ */ new Map(), i(n3.t, function(r3) {
                  n3.R.set(r3, false);
                }), n3.o.clear());
              }, o3.forEach = function(n3, r3) {
                var t2 = this;
                p(this[Q]).forEach(function(e3, i2) {
                  n3.call(r3, t2.get(i2), i2, t2);
                });
              }, o3.get = function(n3) {
                var r3 = this[Q];
                u2(r3);
                var i2 = p(r3).get(n3);
                if (r3.I || !t(i2))
                  return i2;
                if (i2 !== r3.t.get(n3))
                  return i2;
                var o4 = N(r3.A.h, i2, r3);
                return e2(r3), r3.o.set(n3, o4), o4;
              }, o3.keys = function() {
                return p(this[Q]).keys();
              }, o3.values = function() {
                var n3, r3 = this, t2 = this.keys();
                return (n3 = {})[V] = function() {
                  return r3.values();
                }, n3.next = function() {
                  var n4 = t2.next();
                  return n4.done ? n4 : { done: false, value: r3.get(n4.value) };
                }, n3;
              }, o3.entries = function() {
                var n3, r3 = this, t2 = this.keys();
                return (n3 = {})[V] = function() {
                  return r3.entries();
                }, n3.next = function() {
                  var n4 = t2.next();
                  if (n4.done)
                    return n4;
                  var e3 = r3.get(n4.value);
                  return { done: false, value: [n4.value, e3] };
                }, n3;
              }, o3[V] = function() {
                return this.entries();
              }, n2;
            }(), c2 = function() {
              function n2(n3, r3) {
                return this[Q] = { i: 3, l: r3, A: r3 ? r3.A : _(), P: false, I: false, o: void 0, t: n3, k: this, p: /* @__PURE__ */ new Map(), O: false, C: false }, this;
              }
              r2(n2, Set);
              var t2 = n2.prototype;
              return Object.defineProperty(t2, "size", { get: function() {
                return p(this[Q]).size;
              } }), t2.has = function(n3) {
                var r3 = this[Q];
                return u2(r3), r3.o ? !!r3.o.has(n3) || !(!r3.p.has(n3) || !r3.o.has(r3.p.get(n3))) : r3.t.has(n3);
              }, t2.add = function(n3) {
                var r3 = this[Q];
                return u2(r3), this.has(n3) || (o2(r3), k(r3), r3.o.add(n3)), this;
              }, t2.delete = function(n3) {
                if (!this.has(n3))
                  return false;
                var r3 = this[Q];
                return u2(r3), o2(r3), k(r3), r3.o.delete(n3) || !!r3.p.has(n3) && r3.o.delete(r3.p.get(n3));
              }, t2.clear = function() {
                var n3 = this[Q];
                u2(n3), p(n3).size && (o2(n3), k(n3), n3.o.clear());
              }, t2.values = function() {
                var n3 = this[Q];
                return u2(n3), o2(n3), n3.o.values();
              }, t2.entries = function() {
                var n3 = this[Q];
                return u2(n3), o2(n3), n3.o.entries();
              }, t2.keys = function() {
                return this.values();
              }, t2[V] = function() {
                return this.values();
              }, t2.forEach = function(n3, r3) {
                for (var t3 = this.values(), e3 = t3.next(); !e3.done; )
                  n3.call(r3, e3.value, e3.value, this), e3 = t3.next();
              }, n2;
            }();
            m("MapSet", { F: function(n2, r3) {
              return new f2(n2, r3);
            }, T: function(n2, r3) {
              return new c2(n2, r3);
            } });
          }
          function J() {
            F(), C(), T();
          }
          function K(n2) {
            return n2;
          }
          function $(n2) {
            return n2;
          }
          var G, U, W = "undefined" != typeof Symbol && "symbol" == typeof Symbol("x"), X = "undefined" != typeof Map, q = "undefined" != typeof Set, B = "undefined" != typeof Proxy && void 0 !== Proxy.revocable && "undefined" != typeof Reflect, H = W ? Symbol.for("immer-nothing") : ((G = {})["immer-nothing"] = true, G), L = W ? Symbol.for("immer-draftable") : "__$immer_draftable", Q = W ? Symbol.for("immer-state") : "__$immer_state", V = "undefined" != typeof Symbol && Symbol.iterator || "@@iterator", Y = { 0: "Illegal state", 1: "Immer drafts cannot have computed properties", 2: "This object has been frozen and should not be mutated", 3: function(n2) {
            return "Cannot use a proxy that has been revoked. Did you pass an object from inside an immer function to an async process? " + n2;
          }, 4: "An immer producer returned a new value *and* modified its draft. Either return a new value *or* modify the draft.", 5: "Immer forbids circular references", 6: "The first or second argument to `produce` must be a function", 7: "The third argument to `produce` must be a function or undefined", 8: "First argument to `createDraft` must be a plain object, an array, or an immerable object", 9: "First argument to `finishDraft` must be a draft returned by `createDraft`", 10: "The given draft is already finalized", 11: "Object.defineProperty() cannot be used on an Immer draft", 12: "Object.setPrototypeOf() cannot be used on an Immer draft", 13: "Immer only supports deleting array indices", 14: "Immer only supports setting array indices and the 'length' property", 15: function(n2) {
            return "Cannot apply patch, path doesn't resolve: " + n2;
          }, 16: 'Sets cannot have "replace" patches.', 17: function(n2) {
            return "Unsupported patch operation: " + n2;
          }, 18: function(n2) {
            return "The plugin for '" + n2 + "' has not been loaded into Immer. To enable the plugin, import and call `enable" + n2 + "()` when initializing your application.";
          }, 20: "Cannot use proxies if Proxy, Proxy.revocable or Reflect are not available", 21: function(n2) {
            return "produce can only be called on things that are draftable: plain objects, arrays, Map, Set or classes that are marked with '[immerable]: true'. Got '" + n2 + "'";
          }, 22: function(n2) {
            return "'current' expects a draft, got: " + n2;
          }, 23: function(n2) {
            return "'original' expects a draft, got: " + n2;
          }, 24: "Patching reserved attributes like __proto__, prototype and constructor is not allowed" }, Z = "" + Object.prototype.constructor, nn = "undefined" != typeof Reflect && Reflect.ownKeys ? Reflect.ownKeys : void 0 !== Object.getOwnPropertySymbols ? function(n2) {
            return Object.getOwnPropertyNames(n2).concat(Object.getOwnPropertySymbols(n2));
          } : Object.getOwnPropertyNames, rn = Object.getOwnPropertyDescriptors || function(n2) {
            var r2 = {};
            return nn(n2).forEach(function(t2) {
              r2[t2] = Object.getOwnPropertyDescriptor(n2, t2);
            }), r2;
          }, tn = {}, en = { get: function(n2, r2) {
            if (r2 === Q)
              return n2;
            var e2 = p(n2);
            if (!u(e2, r2))
              return function(n3, r3, t2) {
                var e3, i3 = I(r3, t2);
                return i3 ? "value" in i3 ? i3.value : null === (e3 = i3.get) || void 0 === e3 ? void 0 : e3.call(n3.k) : void 0;
              }(n2, e2, r2);
            var i2 = e2[r2];
            return n2.I || !t(i2) ? i2 : i2 === z(n2.t, r2) ? (E(n2), n2.o[r2] = N(n2.A.h, i2, n2)) : i2;
          }, has: function(n2, r2) {
            return r2 in p(n2);
          }, ownKeys: function(n2) {
            return Reflect.ownKeys(p(n2));
          }, set: function(n2, r2, t2) {
            var e2 = I(p(n2), r2);
            if (null == e2 ? void 0 : e2.set)
              return e2.set.call(n2.k, t2), true;
            if (!n2.P) {
              var i2 = z(p(n2), r2), o2 = null == i2 ? void 0 : i2[Q];
              if (o2 && o2.t === t2)
                return n2.o[r2] = t2, n2.R[r2] = false, true;
              if (c(t2, i2) && (void 0 !== t2 || u(n2.t, r2)))
                return true;
              E(n2), k(n2);
            }
            return n2.o[r2] === t2 && (void 0 !== t2 || r2 in n2.o) || Number.isNaN(t2) && Number.isNaN(n2.o[r2]) || (n2.o[r2] = t2, n2.R[r2] = true), true;
          }, deleteProperty: function(n2, r2) {
            return void 0 !== z(n2.t, r2) || r2 in n2.t ? (n2.R[r2] = false, E(n2), k(n2)) : delete n2.R[r2], n2.o && delete n2.o[r2], true;
          }, getOwnPropertyDescriptor: function(n2, r2) {
            var t2 = p(n2), e2 = Reflect.getOwnPropertyDescriptor(t2, r2);
            return e2 ? { writable: true, configurable: 1 !== n2.i || "length" !== r2, enumerable: e2.enumerable, value: t2[r2] } : e2;
          }, defineProperty: function() {
            n(11);
          }, getPrototypeOf: function(n2) {
            return Object.getPrototypeOf(n2.t);
          }, setPrototypeOf: function() {
            n(12);
          } }, on = {};
          i(en, function(n2, r2) {
            on[n2] = function() {
              return arguments[0] = arguments[0][0], r2.apply(this, arguments);
            };
          }), on.deleteProperty = function(r2, t2) {
            return isNaN(parseInt(t2)) && n(13), on.set.call(this, r2, t2, void 0);
          }, on.set = function(r2, t2, e2) {
            return "length" !== t2 && isNaN(parseInt(t2)) && n(14), en.set.call(this, r2[0], t2, e2, r2[0]);
          };
          var un = function() {
            function e2(r2) {
              var e3 = this;
              this.g = B, this.D = true, this.produce = function(r3, i3, o2) {
                if ("function" == typeof r3 && "function" != typeof i3) {
                  var u2 = i3;
                  i3 = r3;
                  var a2 = e3;
                  return function(n2) {
                    var r4 = this;
                    void 0 === n2 && (n2 = u2);
                    for (var t2 = arguments.length, e4 = Array(t2 > 1 ? t2 - 1 : 0), o3 = 1; o3 < t2; o3++)
                      e4[o3 - 1] = arguments[o3];
                    return a2.produce(n2, function(n3) {
                      var t3;
                      return (t3 = i3).call.apply(t3, [r4, n3].concat(e4));
                    });
                  };
                }
                var f2;
                if ("function" != typeof i3 && n(6), void 0 !== o2 && "function" != typeof o2 && n(7), t(r3)) {
                  var c2 = w(e3), s2 = N(e3, r3, void 0), v2 = true;
                  try {
                    f2 = i3(s2), v2 = false;
                  } finally {
                    v2 ? O(c2) : g(c2);
                  }
                  return "undefined" != typeof Promise && f2 instanceof Promise ? f2.then(function(n2) {
                    return j(c2, o2), P(n2, c2);
                  }, function(n2) {
                    throw O(c2), n2;
                  }) : (j(c2, o2), P(f2, c2));
                }
                if (!r3 || "object" != typeof r3) {
                  if (void 0 === (f2 = i3(r3)) && (f2 = r3), f2 === H && (f2 = void 0), e3.D && d(f2, true), o2) {
                    var p2 = [], l2 = [];
                    b("Patches").M(r3, f2, p2, l2), o2(p2, l2);
                  }
                  return f2;
                }
                n(21, r3);
              }, this.produceWithPatches = function(n2, r3) {
                if ("function" == typeof n2)
                  return function(r4) {
                    for (var t3 = arguments.length, i4 = Array(t3 > 1 ? t3 - 1 : 0), o3 = 1; o3 < t3; o3++)
                      i4[o3 - 1] = arguments[o3];
                    return e3.produceWithPatches(r4, function(r5) {
                      return n2.apply(void 0, [r5].concat(i4));
                    });
                  };
                var t2, i3, o2 = e3.produce(n2, r3, function(n3, r4) {
                  t2 = n3, i3 = r4;
                });
                return "undefined" != typeof Promise && o2 instanceof Promise ? o2.then(function(n3) {
                  return [n3, t2, i3];
                }) : [o2, t2, i3];
              }, "boolean" == typeof (null == r2 ? void 0 : r2.useProxies) && this.setUseProxies(r2.useProxies), "boolean" == typeof (null == r2 ? void 0 : r2.autoFreeze) && this.setAutoFreeze(r2.autoFreeze);
            }
            var i2 = e2.prototype;
            return i2.createDraft = function(e3) {
              t(e3) || n(8), r(e3) && (e3 = R(e3));
              var i3 = w(this), o2 = N(this, e3, void 0);
              return o2[Q].C = true, g(i3), o2;
            }, i2.finishDraft = function(r2, t2) {
              var e3 = r2 && r2[Q];
              e3 && e3.C || n(9), e3.I && n(10);
              var i3 = e3.A;
              return j(i3, t2), P(void 0, i3);
            }, i2.setAutoFreeze = function(n2) {
              this.D = n2;
            }, i2.setUseProxies = function(r2) {
              r2 && !B && n(20), this.g = r2;
            }, i2.applyPatches = function(n2, t2) {
              var e3;
              for (e3 = t2.length - 1; e3 >= 0; e3--) {
                var i3 = t2[e3];
                if (0 === i3.path.length && "replace" === i3.op) {
                  n2 = i3.value;
                  break;
                }
              }
              e3 > -1 && (t2 = t2.slice(e3 + 1));
              var o2 = b("Patches").$;
              return r(n2) ? o2(n2, t2) : this.produce(n2, function(n3) {
                return o2(n3, t2);
              });
            }, e2;
          }(), an = new un(), fn = an.produce, cn = an.produceWithPatches.bind(an), sn = an.setAutoFreeze.bind(an), vn = an.setUseProxies.bind(an), pn = an.applyPatches.bind(an), ln = an.createDraft.bind(an), dn = an.finishDraft.bind(an);
          const __WEBPACK_DEFAULT_EXPORT__ = fn;
        }
      ),
      /***/
      "./node_modules/lit-html/development/lit-html.js": (
        /*!*******************************************************!*\
          !*** ./node_modules/lit-html/development/lit-html.js ***!
          \*******************************************************/
        /***/
        (__unused_webpack___webpack_module__, __webpack_exports__2, __webpack_require__2) => {
          __webpack_require__2.r(__webpack_exports__2);
          __webpack_require__2.d(__webpack_exports__2, {
            /* harmony export */
            "_$LH": () => (
              /* binding */
              _$LH
            ),
            /* harmony export */
            "html": () => (
              /* binding */
              html
            ),
            /* harmony export */
            "noChange": () => (
              /* binding */
              noChange
            ),
            /* harmony export */
            "nothing": () => (
              /* binding */
              nothing
            ),
            /* harmony export */
            "render": () => (
              /* binding */
              render
            ),
            /* harmony export */
            "svg": () => (
              /* binding */
              svg
            )
            /* harmony export */
          });
          var _a, _b, _c, _d;
          const DEV_MODE = true;
          const ENABLE_EXTRA_SECURITY_HOOKS = true;
          const ENABLE_SHADYDOM_NOPATCH = true;
          const NODE_MODE = false;
          const global = NODE_MODE ? globalThis : window;
          const debugLogEvent = DEV_MODE ? (event) => {
            const shouldEmit = global.emitLitDebugLogEvents;
            if (!shouldEmit) {
              return;
            }
            global.dispatchEvent(new CustomEvent("lit-debug", {
              detail: event
            }));
          } : void 0;
          let debugLogRenderId = 0;
          let issueWarning;
          if (DEV_MODE) {
            (_a = global.litIssuedWarnings) !== null && _a !== void 0 ? _a : global.litIssuedWarnings = /* @__PURE__ */ new Set();
            issueWarning = (code, warning) => {
              warning += code ? ` See https://lit.dev/msg/${code} for more information.` : "";
              if (!global.litIssuedWarnings.has(warning)) {
                console.warn(warning);
                global.litIssuedWarnings.add(warning);
              }
            };
            issueWarning("dev-mode", `Lit is in dev mode. Not recommended for production!`);
          }
          const wrap = ENABLE_SHADYDOM_NOPATCH && ((_b = global.ShadyDOM) === null || _b === void 0 ? void 0 : _b.inUse) && ((_c = global.ShadyDOM) === null || _c === void 0 ? void 0 : _c.noPatch) === true ? global.ShadyDOM.wrap : (node) => node;
          const trustedTypes = global.trustedTypes;
          const policy = trustedTypes ? trustedTypes.createPolicy("lit-html", {
            createHTML: (s) => s
          }) : void 0;
          const identityFunction = (value) => value;
          const noopSanitizer = (_node, _name, _type) => identityFunction;
          const setSanitizer = (newSanitizer) => {
            if (!ENABLE_EXTRA_SECURITY_HOOKS) {
              return;
            }
            if (sanitizerFactoryInternal !== noopSanitizer) {
              throw new Error(`Attempted to overwrite existing lit-html security policy. setSanitizeDOMValueFactory should be called at most once.`);
            }
            sanitizerFactoryInternal = newSanitizer;
          };
          const _testOnlyClearSanitizerFactoryDoNotCallOrElse = () => {
            sanitizerFactoryInternal = noopSanitizer;
          };
          const createSanitizer = (node, name, type) => {
            return sanitizerFactoryInternal(node, name, type);
          };
          const boundAttributeSuffix = "$lit$";
          const marker = `lit$${String(Math.random()).slice(9)}$`;
          const markerMatch = "?" + marker;
          const nodeMarker = `<${markerMatch}>`;
          const d = NODE_MODE && global.document === void 0 ? {
            createTreeWalker() {
              return {};
            }
          } : document;
          const createMarker = (v = "") => d.createComment(v);
          const isPrimitive = (value) => value === null || typeof value != "object" && typeof value != "function";
          const isArray = Array.isArray;
          const isIterable = (value) => isArray(value) || // eslint-disable-next-line @typescript-eslint/no-explicit-any
          typeof (value === null || value === void 0 ? void 0 : value[Symbol.iterator]) === "function";
          const SPACE_CHAR = `[ 	
\f\r]`;
          const ATTR_VALUE_CHAR = `[^ 	
\f\r"'\`<>=]`;
          const NAME_CHAR = `[^\\s"'>=/]`;
          const textEndRegex = /<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g;
          const COMMENT_START = 1;
          const TAG_NAME = 2;
          const DYNAMIC_TAG_NAME = 3;
          const commentEndRegex = /-->/g;
          const comment2EndRegex = />/g;
          const tagEndRegex = new RegExp(`>|${SPACE_CHAR}(?:(${NAME_CHAR}+)(${SPACE_CHAR}*=${SPACE_CHAR}*(?:${ATTR_VALUE_CHAR}|("|')|))|$)`, "g");
          const ENTIRE_MATCH = 0;
          const ATTRIBUTE_NAME = 1;
          const SPACES_AND_EQUALS = 2;
          const QUOTE_CHAR = 3;
          const singleQuoteAttrEndRegex = /'/g;
          const doubleQuoteAttrEndRegex = /"/g;
          const rawTextElement = /^(?:script|style|textarea|title)$/i;
          const HTML_RESULT = 1;
          const SVG_RESULT = 2;
          const ATTRIBUTE_PART = 1;
          const CHILD_PART = 2;
          const PROPERTY_PART = 3;
          const BOOLEAN_ATTRIBUTE_PART = 4;
          const EVENT_PART = 5;
          const ELEMENT_PART = 6;
          const COMMENT_PART = 7;
          const tag = (type) => (strings, ...values) => {
            if (DEV_MODE && strings.some((s) => s === void 0)) {
              console.warn("Some template strings are undefined.\nThis is probably caused by illegal octal escape sequences.");
            }
            return {
              // This property needs to remain unminified.
              ["_$litType$"]: type,
              strings,
              values
            };
          };
          const html = tag(HTML_RESULT);
          const svg = tag(SVG_RESULT);
          const noChange = Symbol.for("lit-noChange");
          const nothing = Symbol.for("lit-nothing");
          const templateCache = /* @__PURE__ */ new WeakMap();
          const walker = d.createTreeWalker(d, 129, null, false);
          let sanitizerFactoryInternal = noopSanitizer;
          const getTemplateHtml = (strings, type) => {
            const l = strings.length - 1;
            const attrNames = [];
            let html2 = type === SVG_RESULT ? "<svg>" : "";
            let rawTextEndRegex;
            let regex = textEndRegex;
            for (let i = 0; i < l; i++) {
              const s = strings[i];
              let attrNameEndIndex = -1;
              let attrName;
              let lastIndex = 0;
              let match;
              while (lastIndex < s.length) {
                regex.lastIndex = lastIndex;
                match = regex.exec(s);
                if (match === null) {
                  break;
                }
                lastIndex = regex.lastIndex;
                if (regex === textEndRegex) {
                  if (match[COMMENT_START] === "!--") {
                    regex = commentEndRegex;
                  } else if (match[COMMENT_START] !== void 0) {
                    regex = comment2EndRegex;
                  } else if (match[TAG_NAME] !== void 0) {
                    if (rawTextElement.test(match[TAG_NAME])) {
                      rawTextEndRegex = new RegExp(`</${match[TAG_NAME]}`, "g");
                    }
                    regex = tagEndRegex;
                  } else if (match[DYNAMIC_TAG_NAME] !== void 0) {
                    if (DEV_MODE) {
                      throw new Error("Bindings in tag names are not supported. Please use static templates instead. See https://lit.dev/docs/templates/expressions/#static-expressions");
                    }
                    regex = tagEndRegex;
                  }
                } else if (regex === tagEndRegex) {
                  if (match[ENTIRE_MATCH] === ">") {
                    regex = rawTextEndRegex !== null && rawTextEndRegex !== void 0 ? rawTextEndRegex : textEndRegex;
                    attrNameEndIndex = -1;
                  } else if (match[ATTRIBUTE_NAME] === void 0) {
                    attrNameEndIndex = -2;
                  } else {
                    attrNameEndIndex = regex.lastIndex - match[SPACES_AND_EQUALS].length;
                    attrName = match[ATTRIBUTE_NAME];
                    regex = match[QUOTE_CHAR] === void 0 ? tagEndRegex : match[QUOTE_CHAR] === '"' ? doubleQuoteAttrEndRegex : singleQuoteAttrEndRegex;
                  }
                } else if (regex === doubleQuoteAttrEndRegex || regex === singleQuoteAttrEndRegex) {
                  regex = tagEndRegex;
                } else if (regex === commentEndRegex || regex === comment2EndRegex) {
                  regex = textEndRegex;
                } else {
                  regex = tagEndRegex;
                  rawTextEndRegex = void 0;
                }
              }
              if (DEV_MODE) {
                console.assert(attrNameEndIndex === -1 || regex === tagEndRegex || regex === singleQuoteAttrEndRegex || regex === doubleQuoteAttrEndRegex, "unexpected parse state B");
              }
              const end = regex === tagEndRegex && strings[i + 1].startsWith("/>") ? " " : "";
              html2 += regex === textEndRegex ? s + nodeMarker : attrNameEndIndex >= 0 ? (attrNames.push(attrName), s.slice(0, attrNameEndIndex) + boundAttributeSuffix + s.slice(attrNameEndIndex)) + marker + end : s + marker + (attrNameEndIndex === -2 ? (attrNames.push(void 0), i) : end);
            }
            const htmlResult = html2 + (strings[l] || "<?>") + (type === SVG_RESULT ? "</svg>" : "");
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
        `.trim().replace(/\n */g, "\n");
              }
              throw new Error(message);
            }
            return [
              policy !== void 0 ? policy.createHTML(htmlResult) : htmlResult,
              attrNames
            ];
          };
          class Template {
            constructor({ strings, ["_$litType$"]: type }, options) {
              this.parts = [];
              let node;
              let nodeIndex = 0;
              let attrNameIndex = 0;
              const partCount = strings.length - 1;
              const parts = this.parts;
              const [html2, attrNames] = getTemplateHtml(strings, type);
              this.el = Template.createElement(html2, options);
              walker.currentNode = this.el.content;
              if (type === SVG_RESULT) {
                const content = this.el.content;
                const svgElement = content.firstChild;
                svgElement.remove();
                content.append(...svgElement.childNodes);
              }
              while ((node = walker.nextNode()) !== null && parts.length < partCount) {
                if (node.nodeType === 1) {
                  if (DEV_MODE) {
                    const tag2 = node.localName;
                    if (/^(?:textarea|template)$/i.test(tag2) && node.innerHTML.includes(marker)) {
                      const m = `Expressions are not supported inside \`${tag2}\` elements. See https://lit.dev/msg/expression-in-${tag2} for more information.`;
                      if (tag2 === "template") {
                        throw new Error(m);
                      } else
                        issueWarning("", m);
                    }
                  }
                  if (node.hasAttributes()) {
                    const attrsToRemove = [];
                    for (const name of node.getAttributeNames()) {
                      if (name.endsWith(boundAttributeSuffix) || name.startsWith(marker)) {
                        const realName = attrNames[attrNameIndex++];
                        attrsToRemove.push(name);
                        if (realName !== void 0) {
                          const value = node.getAttribute(realName.toLowerCase() + boundAttributeSuffix);
                          const statics = value.split(marker);
                          const m = /([.?@])?(.*)/.exec(realName);
                          parts.push({
                            type: ATTRIBUTE_PART,
                            index: nodeIndex,
                            name: m[2],
                            strings: statics,
                            ctor: m[1] === "." ? PropertyPart : m[1] === "?" ? BooleanAttributePart : m[1] === "@" ? EventPart : AttributePart
                          });
                        } else {
                          parts.push({
                            type: ELEMENT_PART,
                            index: nodeIndex
                          });
                        }
                      }
                    }
                    for (const name of attrsToRemove) {
                      node.removeAttribute(name);
                    }
                  }
                  if (rawTextElement.test(node.tagName)) {
                    const strings2 = node.textContent.split(marker);
                    const lastIndex = strings2.length - 1;
                    if (lastIndex > 0) {
                      node.textContent = trustedTypes ? trustedTypes.emptyScript : "";
                      for (let i = 0; i < lastIndex; i++) {
                        node.append(strings2[i], createMarker());
                        walker.nextNode();
                        parts.push({ type: CHILD_PART, index: ++nodeIndex });
                      }
                      node.append(strings2[lastIndex], createMarker());
                    }
                  }
                } else if (node.nodeType === 8) {
                  const data = node.data;
                  if (data === markerMatch) {
                    parts.push({ type: CHILD_PART, index: nodeIndex });
                  } else {
                    let i = -1;
                    while ((i = node.data.indexOf(marker, i + 1)) !== -1) {
                      parts.push({ type: COMMENT_PART, index: nodeIndex });
                      i += marker.length - 1;
                    }
                  }
                }
                nodeIndex++;
              }
              debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                kind: "template prep",
                template: this,
                clonableTemplate: this.el,
                parts: this.parts,
                strings
              });
            }
            // Overridden via `litHtmlPolyfillSupport` to provide platform support.
            /** @nocollapse */
            static createElement(html2, _options) {
              const el = d.createElement("template");
              el.innerHTML = html2;
              return el;
            }
          }
          function resolveDirective(part, value, parent = part, attributeIndex) {
            var _a2, _b2, _c2;
            var _d2;
            if (value === noChange) {
              return value;
            }
            let currentDirective = attributeIndex !== void 0 ? (_a2 = parent.__directives) === null || _a2 === void 0 ? void 0 : _a2[attributeIndex] : parent.__directive;
            const nextDirectiveConstructor = isPrimitive(value) ? void 0 : (
              // This property needs to remain unminified.
              value["_$litDirective$"]
            );
            if ((currentDirective === null || currentDirective === void 0 ? void 0 : currentDirective.constructor) !== nextDirectiveConstructor) {
              (_b2 = currentDirective === null || currentDirective === void 0 ? void 0 : currentDirective["_$notifyDirectiveConnectionChanged"]) === null || _b2 === void 0 ? void 0 : _b2.call(currentDirective, false);
              if (nextDirectiveConstructor === void 0) {
                currentDirective = void 0;
              } else {
                currentDirective = new nextDirectiveConstructor(part);
                currentDirective._$initialize(part, parent, attributeIndex);
              }
              if (attributeIndex !== void 0) {
                ((_c2 = (_d2 = parent).__directives) !== null && _c2 !== void 0 ? _c2 : _d2.__directives = [])[attributeIndex] = currentDirective;
              } else {
                parent.__directive = currentDirective;
              }
            }
            if (currentDirective !== void 0) {
              value = resolveDirective(part, currentDirective._$resolve(part, value.values), currentDirective, attributeIndex);
            }
            return value;
          }
          class TemplateInstance {
            constructor(template, parent) {
              this._parts = [];
              this._$disconnectableChildren = void 0;
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
              var _a2;
              const { el: { content }, parts } = this._$template;
              const fragment = ((_a2 = options === null || options === void 0 ? void 0 : options.creationScope) !== null && _a2 !== void 0 ? _a2 : d).importNode(content, true);
              walker.currentNode = fragment;
              let node = walker.nextNode();
              let nodeIndex = 0;
              let partIndex = 0;
              let templatePart = parts[0];
              while (templatePart !== void 0) {
                if (nodeIndex === templatePart.index) {
                  let part;
                  if (templatePart.type === CHILD_PART) {
                    part = new ChildPart(node, node.nextSibling, this, options);
                  } else if (templatePart.type === ATTRIBUTE_PART) {
                    part = new templatePart.ctor(node, templatePart.name, templatePart.strings, this, options);
                  } else if (templatePart.type === ELEMENT_PART) {
                    part = new ElementPart(node, this, options);
                  }
                  this._parts.push(part);
                  templatePart = parts[++partIndex];
                }
                if (nodeIndex !== (templatePart === null || templatePart === void 0 ? void 0 : templatePart.index)) {
                  node = walker.nextNode();
                  nodeIndex++;
                }
              }
              return fragment;
            }
            _update(values) {
              let i = 0;
              for (const part of this._parts) {
                if (part !== void 0) {
                  debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                    kind: "set part",
                    part,
                    value: values[i],
                    valueIndex: i,
                    values,
                    templateInstance: this
                  });
                  if (part.strings !== void 0) {
                    part._$setValue(values, part, i);
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
              var _a2;
              this.type = CHILD_PART;
              this._$committedValue = nothing;
              this._$disconnectableChildren = void 0;
              this._$startNode = startNode;
              this._$endNode = endNode;
              this._$parent = parent;
              this.options = options;
              this.__isConnected = (_a2 = options === null || options === void 0 ? void 0 : options.isConnected) !== null && _a2 !== void 0 ? _a2 : true;
              if (ENABLE_EXTRA_SECURITY_HOOKS) {
                this._textSanitizer = void 0;
              }
            }
            // See comment in Disconnectable interface for why this is a getter
            get _$isConnected() {
              var _a2, _b2;
              return (_b2 = (_a2 = this._$parent) === null || _a2 === void 0 ? void 0 : _a2._$isConnected) !== null && _b2 !== void 0 ? _b2 : this.__isConnected;
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
              if (parent !== void 0 && parentNode.nodeType === 11) {
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
              var _a2;
              if (DEV_MODE && this.parentNode === null) {
                throw new Error(`This \`ChildPart\` has no \`parentNode\` and therefore cannot accept a value. This likely means the element containing the part was manipulated in an unsupported way outside of Lit's control such that the part's marker nodes were ejected from DOM. For example, setting the element's \`innerHTML\` or \`textContent\` can do this.`);
              }
              value = resolveDirective(this, value, directiveParent);
              if (isPrimitive(value)) {
                if (value === nothing || value == null || value === "") {
                  if (this._$committedValue !== nothing) {
                    debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                      kind: "commit nothing to child",
                      start: this._$startNode,
                      end: this._$endNode,
                      parent: this._$parent,
                      options: this.options
                    });
                    this._$clear();
                  }
                  this._$committedValue = nothing;
                } else if (value !== this._$committedValue && value !== noChange) {
                  this._commitText(value);
                }
              } else if (value["_$litType$"] !== void 0) {
                this._commitTemplateResult(value);
              } else if (value.nodeType !== void 0) {
                if (DEV_MODE && ((_a2 = this.options) === null || _a2 === void 0 ? void 0 : _a2.host) === value) {
                  this._commitText(`[probable mistake: rendered a template's host in itself (commonly caused by writing \${this} in a template]`);
                  console.warn(`Attempted to render the template host`, value, `inside itself. This is almost always a mistake, and in dev mode `, `we render some warning text. In production however, we'll `, `render it, which will usually result in an error, and sometimes `, `in the element disappearing from the DOM.`);
                  return;
                }
                this._commitNode(value);
              } else if (isIterable(value)) {
                this._commitIterable(value);
              } else {
                this._commitText(value);
              }
            }
            _insert(node, ref = this._$endNode) {
              return wrap(wrap(this._$startNode).parentNode).insertBefore(node, ref);
            }
            _commitNode(value) {
              var _a2;
              if (this._$committedValue !== value) {
                this._$clear();
                if (ENABLE_EXTRA_SECURITY_HOOKS && sanitizerFactoryInternal !== noopSanitizer) {
                  const parentNodeName = (_a2 = this._$startNode.parentNode) === null || _a2 === void 0 ? void 0 : _a2.nodeName;
                  if (parentNodeName === "STYLE" || parentNodeName === "SCRIPT") {
                    let message = "Forbidden";
                    if (DEV_MODE) {
                      if (parentNodeName === "STYLE") {
                        message = `Lit does not support binding inside style nodes. This is a security risk, as style injection attacks can exfiltrate data and spoof UIs. Consider instead using css\`...\` literals to compose styles, and make do dynamic styling with css custom properties, ::parts, <slot>s, and by mutating the DOM rather than stylesheets.`;
                      } else {
                        message = `Lit does not support binding inside script nodes. This is a security risk, as it could allow arbitrary code execution.`;
                      }
                    }
                    throw new Error(message);
                  }
                }
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "commit node",
                  start: this._$startNode,
                  parent: this._$parent,
                  value,
                  options: this.options
                });
                this._$committedValue = this._insert(value);
              }
            }
            _commitText(value) {
              if (this._$committedValue !== nothing && isPrimitive(this._$committedValue)) {
                const node = wrap(this._$startNode).nextSibling;
                if (ENABLE_EXTRA_SECURITY_HOOKS) {
                  if (this._textSanitizer === void 0) {
                    this._textSanitizer = createSanitizer(node, "data", "property");
                  }
                  value = this._textSanitizer(value);
                }
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "commit text",
                  node,
                  value,
                  options: this.options
                });
                node.data = value;
              } else {
                if (ENABLE_EXTRA_SECURITY_HOOKS) {
                  const textNode = document.createTextNode("");
                  this._commitNode(textNode);
                  if (this._textSanitizer === void 0) {
                    this._textSanitizer = createSanitizer(textNode, "data", "property");
                  }
                  value = this._textSanitizer(value);
                  debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                    kind: "commit text",
                    node: textNode,
                    value,
                    options: this.options
                  });
                  textNode.data = value;
                } else {
                  this._commitNode(d.createTextNode(value));
                  debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                    kind: "commit text",
                    node: wrap(this._$startNode).nextSibling,
                    value,
                    options: this.options
                  });
                }
              }
              this._$committedValue = value;
            }
            _commitTemplateResult(result) {
              var _a2;
              const { values, ["_$litType$"]: type } = result;
              const template = typeof type === "number" ? this._$getTemplate(result) : (type.el === void 0 && (type.el = Template.createElement(type.h, this.options)), type);
              if (((_a2 = this._$committedValue) === null || _a2 === void 0 ? void 0 : _a2._$template) === template) {
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "template updating",
                  template,
                  instance: this._$committedValue,
                  parts: this._$committedValue._parts,
                  options: this.options,
                  values
                });
                this._$committedValue._update(values);
              } else {
                const instance = new TemplateInstance(template, this);
                const fragment = instance._clone(this.options);
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "template instantiated",
                  template,
                  instance,
                  parts: instance._parts,
                  options: this.options,
                  fragment,
                  values
                });
                instance._update(values);
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "template instantiated and updated",
                  template,
                  instance,
                  parts: instance._parts,
                  options: this.options,
                  fragment,
                  values
                });
                this._commitNode(fragment);
                this._$committedValue = instance;
              }
            }
            // Overridden via `litHtmlPolyfillSupport` to provide platform support.
            /** @internal */
            _$getTemplate(result) {
              let template = templateCache.get(result.strings);
              if (template === void 0) {
                templateCache.set(result.strings, template = new Template(result));
              }
              return template;
            }
            _commitIterable(value) {
              if (!isArray(this._$committedValue)) {
                this._$committedValue = [];
                this._$clear();
              }
              const itemParts = this._$committedValue;
              let partIndex = 0;
              let itemPart;
              for (const item of value) {
                if (partIndex === itemParts.length) {
                  itemParts.push(itemPart = new ChildPart(this._insert(createMarker()), this._insert(createMarker()), this, this.options));
                } else {
                  itemPart = itemParts[partIndex];
                }
                itemPart._$setValue(item);
                partIndex++;
              }
              if (partIndex < itemParts.length) {
                this._$clear(itemPart && wrap(itemPart._$endNode).nextSibling, partIndex);
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
              var _a2;
              (_a2 = this._$notifyConnectionChanged) === null || _a2 === void 0 ? void 0 : _a2.call(this, false, true, from);
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
              var _a2;
              if (this._$parent === void 0) {
                this.__isConnected = isConnected;
                (_a2 = this._$notifyConnectionChanged) === null || _a2 === void 0 ? void 0 : _a2.call(this, isConnected);
              } else if (DEV_MODE) {
                throw new Error("part.setConnected() may only be called on a RootPart returned from render().");
              }
            }
          }
          class AttributePart {
            constructor(element, name, strings, parent, options) {
              this.type = ATTRIBUTE_PART;
              this._$committedValue = nothing;
              this._$disconnectableChildren = void 0;
              this.element = element;
              this.name = name;
              this._$parent = parent;
              this.options = options;
              if (strings.length > 2 || strings[0] !== "" || strings[1] !== "") {
                this._$committedValue = new Array(strings.length - 1).fill(new String());
                this.strings = strings;
              } else {
                this._$committedValue = nothing;
              }
              if (ENABLE_EXTRA_SECURITY_HOOKS) {
                this._sanitizer = void 0;
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
              let change = false;
              if (strings === void 0) {
                value = resolveDirective(this, value, directiveParent, 0);
                change = !isPrimitive(value) || value !== this._$committedValue && value !== noChange;
                if (change) {
                  this._$committedValue = value;
                }
              } else {
                const values = value;
                value = strings[0];
                let i, v;
                for (i = 0; i < strings.length - 1; i++) {
                  v = resolveDirective(this, values[valueIndex + i], directiveParent, i);
                  if (v === noChange) {
                    v = this._$committedValue[i];
                  }
                  change || (change = !isPrimitive(v) || v !== this._$committedValue[i]);
                  if (v === nothing) {
                    value = nothing;
                  } else if (value !== nothing) {
                    value += (v !== null && v !== void 0 ? v : "") + strings[i + 1];
                  }
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
                  if (this._sanitizer === void 0) {
                    this._sanitizer = sanitizerFactoryInternal(this.element, this.name, "attribute");
                  }
                  value = this._sanitizer(value !== null && value !== void 0 ? value : "");
                }
                debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                  kind: "commit attribute",
                  element: this.element,
                  name: this.name,
                  value,
                  options: this.options
                });
                wrap(this.element).setAttribute(this.name, value !== null && value !== void 0 ? value : "");
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
                if (this._sanitizer === void 0) {
                  this._sanitizer = sanitizerFactoryInternal(this.element, this.name, "property");
                }
                value = this._sanitizer(value);
              }
              debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                kind: "commit property",
                element: this.element,
                name: this.name,
                value,
                options: this.options
              });
              this.element[this.name] = value === nothing ? void 0 : value;
            }
          }
          const emptyStringForBooleanAttribute = trustedTypes ? trustedTypes.emptyScript : "";
          class BooleanAttributePart extends AttributePart {
            constructor() {
              super(...arguments);
              this.type = BOOLEAN_ATTRIBUTE_PART;
            }
            /** @internal */
            _commitValue(value) {
              debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                kind: "commit boolean attribute",
                element: this.element,
                name: this.name,
                value: !!(value && value !== nothing),
                options: this.options
              });
              if (value && value !== nothing) {
                wrap(this.element).setAttribute(this.name, emptyStringForBooleanAttribute);
              } else {
                wrap(this.element).removeAttribute(this.name);
              }
            }
          }
          class EventPart extends AttributePart {
            constructor(element, name, strings, parent, options) {
              super(element, name, strings, parent, options);
              this.type = EVENT_PART;
              if (DEV_MODE && this.strings !== void 0) {
                throw new Error(`A \`<${element.localName}>\` has a \`@${name}=...\` listener with invalid content. Event listeners in templates must have exactly one expression and no surrounding text.`);
              }
            }
            // EventPart does not use the base _$setValue/_resolveValue implementation
            // since the dirty checking is more complex
            /** @internal */
            _$setValue(newListener, directiveParent = this) {
              var _a2;
              newListener = (_a2 = resolveDirective(this, newListener, directiveParent, 0)) !== null && _a2 !== void 0 ? _a2 : nothing;
              if (newListener === noChange) {
                return;
              }
              const oldListener = this._$committedValue;
              const shouldRemoveListener = newListener === nothing && oldListener !== nothing || newListener.capture !== oldListener.capture || newListener.once !== oldListener.once || newListener.passive !== oldListener.passive;
              const shouldAddListener = newListener !== nothing && (oldListener === nothing || shouldRemoveListener);
              debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                kind: "commit event listener",
                element: this.element,
                name: this.name,
                value: newListener,
                options: this.options,
                removeListener: shouldRemoveListener,
                addListener: shouldAddListener,
                oldListener
              });
              if (shouldRemoveListener) {
                this.element.removeEventListener(this.name, this, oldListener);
              }
              if (shouldAddListener) {
                this.element.addEventListener(this.name, this, newListener);
              }
              this._$committedValue = newListener;
            }
            handleEvent(event) {
              var _a2, _b2;
              if (typeof this._$committedValue === "function") {
                this._$committedValue.call((_b2 = (_a2 = this.options) === null || _a2 === void 0 ? void 0 : _a2.host) !== null && _b2 !== void 0 ? _b2 : this.element, event);
              } else {
                this._$committedValue.handleEvent(event);
              }
            }
          }
          class ElementPart {
            constructor(element, parent, options) {
              this.element = element;
              this.type = ELEMENT_PART;
              this._$disconnectableChildren = void 0;
              this._$parent = parent;
              this.options = options;
            }
            // See comment in Disconnectable interface for why this is a getter
            get _$isConnected() {
              return this._$parent._$isConnected;
            }
            _$setValue(value) {
              debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
                kind: "commit to element binding",
                element: this.element,
                value,
                options: this.options
              });
              resolveDirective(this, value);
            }
          }
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
            _ElementPart: ElementPart
          };
          const polyfillSupport = DEV_MODE ? global.litHtmlPolyfillSupportDevMode : global.litHtmlPolyfillSupport;
          polyfillSupport === null || polyfillSupport === void 0 ? void 0 : polyfillSupport(Template, ChildPart);
          ((_d = global.litHtmlVersions) !== null && _d !== void 0 ? _d : global.litHtmlVersions = []).push("2.6.1");
          if (DEV_MODE && global.litHtmlVersions.length > 1) {
            issueWarning("multiple-versions", `Multiple versions of Lit loaded. Loading multiple versions is not recommended.`);
          }
          const render = (value, container, options) => {
            var _a2, _b2;
            if (DEV_MODE && container == null) {
              throw new TypeError(`The container to render into may not be ${container}`);
            }
            const renderId = DEV_MODE ? debugLogRenderId++ : 0;
            const partOwnerNode = (_a2 = options === null || options === void 0 ? void 0 : options.renderBefore) !== null && _a2 !== void 0 ? _a2 : container;
            let part = partOwnerNode["_$litPart$"];
            debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
              kind: "begin render",
              id: renderId,
              value,
              container,
              options,
              part
            });
            if (part === void 0) {
              const endNode = (_b2 = options === null || options === void 0 ? void 0 : options.renderBefore) !== null && _b2 !== void 0 ? _b2 : null;
              partOwnerNode["_$litPart$"] = part = new ChildPart(container.insertBefore(createMarker(), endNode), endNode, void 0, options !== null && options !== void 0 ? options : {});
            }
            part._$setValue(value);
            debugLogEvent === null || debugLogEvent === void 0 ? void 0 : debugLogEvent({
              kind: "end render",
              id: renderId,
              value,
              container,
              options,
              part
            });
            return part;
          };
          if (ENABLE_EXTRA_SECURITY_HOOKS) {
            render.setSanitizer = setSanitizer;
            render.createSanitizer = createSanitizer;
            if (DEV_MODE) {
              render._testOnlyClearSanitizerFactoryDoNotCallOrElse = _testOnlyClearSanitizerFactoryDoNotCallOrElse;
            }
          }
        }
      )
      /******/
    };
    var __webpack_module_cache__ = {};
    function __webpack_require__(moduleId) {
      var cachedModule = __webpack_module_cache__[moduleId];
      if (cachedModule !== void 0) {
        return cachedModule.exports;
      }
      var module = __webpack_module_cache__[moduleId] = {
        /******/
        // no module.id needed
        /******/
        // no module.loaded needed
        /******/
        exports: {}
        /******/
      };
      __webpack_modules__[moduleId](module, module.exports, __webpack_require__);
      return module.exports;
    }
    (() => {
      __webpack_require__.d = (exports, definition) => {
        for (var key in definition) {
          if (__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
            Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
          }
        }
      };
    })();
    (() => {
      __webpack_require__.o = (obj, prop) => Object.prototype.hasOwnProperty.call(obj, prop);
    })();
    (() => {
      __webpack_require__.r = (exports) => {
        if (typeof Symbol !== "undefined" && Symbol.toStringTag) {
          Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
        }
        Object.defineProperty(exports, "__esModule", { value: true });
      };
    })();
    var __webpack_exports__ = {};
    (() => {
      __webpack_require__.r(__webpack_exports__);
      var _components_app_component__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(
        /*! ./components/app-component */
        "./src/components/app-component.ts"
      );
      const title = document.querySelector("title");
      title.textContent = "User Table";
      const body = document.querySelector("body");
      const appComponent = document.createElement("app-component");
      body.appendChild(appComponent);
    })();
  })();
})();
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
/*!**********************!*\
  !*** ./src/index.ts ***!
  \**********************/
