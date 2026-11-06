import { $n as Output, $o as ɵɵloadQuery, A as contentChild, Ac as Injector, Al as ɵɵdefineInjectable, Bt as computed, Ca as ɵɵclassProp, Cc as EventEmitter, Co as ɵɵelementStart, Cs as ɵɵreference, Da as ɵɵconditionalCreate, Dn as Host, Dr as ViewEncapsulation, En as ElementRef, Fn as Injectable, Gl as operate, Gs as ɵɵtemplate, In as Input, Js as ɵɵtextInterpolate, Kc as RuntimeError, Kl as Observable, Ks as ɵɵtemplateRefExtractor, Mr as afterNextRender, O as booleanAttribute, Oa as ɵɵcontentQuery, Pn as Inject, Ql as Subscription, Qn as Optional, Qo as ɵɵlistener, Rc as NgZone, Rl as ɵɵnamespaceSVG, S as ViewChild, Sa as ɵɵclassMap, So as ɵɵelementEnd, T as afterRenderEffect, Ta as ɵɵconditional, Tl as signal, Ul as Subject, Vl as map, Wi as setClassMetadata, Wl as createOperatorSubscriber, Wt as linkedSignal, X as input, Xc as Version, _c as DOCUMENT, a as ContentChildren, aa as ɵɵControlFeature, al as formatRuntimeError, ao as ɵɵdefineNgModule, ar as RendererFactory2, au as __extends, bi as isSubscribable, bs as ɵɵqueryAdvance, ca as ɵɵInheritDefinitionFeature, cn as Component, co as ɵɵdirectiveInject, cs as ɵɵprojectionDef, d as HOST_TAG_NAME, da as ɵɵadvance, dc as ɵɵviewQuerySignal, dl as inject, dr as Service, es as ɵɵnextContext, go as ɵɵdomProperty, gt as viewChild, hc as CSP_NONCE, i as ContentChild, il as effect, io as ɵɵdefineDirective, ir as Renderer2, jl as ɵɵdefineInjector, jo as ɵɵgetInheritedFactory, ka as ɵɵcontentQuerySignal, kc as InjectionToken, la as ɵɵNgOnChangesFeature, lo as ɵɵdomElement, ls as ɵɵproperty, ml as isSignal, mo as ɵɵdomElementStart, nr as QueryList, ol as forwardRef, pc as APP_ID, po as ɵɵdomElementEnd, pr as SkipSelf, qn as NgModule, qs as ɵɵtext, qt as untracked, r as ChangeDetectorRef, ro as ɵɵdefineComponent, rt as numberAttribute, so as ɵɵdefineService, ss as ɵɵprojection, tn as ApplicationRef, ua as ɵɵProvidersFeature, uc as ɵɵviewQuery, ur as Self, vc as DestroyRef, vo as ɵɵelement, wn as Directive, xs as ɵɵqueryRefresh, ya as ɵɵattribute, yi as isPromise } from "./core-DziHNsPL.js";
import { a as innerFrom, c as popScheduler, o as popNumber, r as from, t as filter } from "./filter-qDZ6zTip.js";
import { n as share, r as forkJoin, t as tap } from "./tap-C_0m_arH.js";
import { Ut as getDOM, Z as NgTemplateOutlet } from "./common-BTusbWHZ.js";
import { a as startWith, c as mergeAll, i as takeUntil, l as dateTimestampProvider, s as debounceTime, t as _animationsDisabled } from "./_animation-chunk-VBlyAsCE.js";
import { I as switchMap } from "./platform-browser-CGC8iabR.js";
import { a as _CdkPrivateStyleLoader, i as MAT_RIPPLE_GLOBAL_OPTIONS, n as _StructuralStylesLoader, o as FocusMonitor, r as MatRippleLoader, t as MatRippleModule } from "./_ripple-module-chunk-DkAEoA5k.js";
import { t as _VisuallyHiddenLoader } from "./private-efNME-fW.js";
import { a as EMPTY$1 } from "./take-RK3DePZ7.js";
import { t as Platform } from "./_platform-chunk-gJYmJ61Q.js";
import { i as Directionality, t as BidiModule } from "./bidi-Dp-YAGew.js";
import "./platform-2RV-NTNb.js";
// node_modules/rxjs/dist/esm5/internal/ReplaySubject.js
var ReplaySubject = function(_super) {
	__extends(ReplaySubject, _super);
	function ReplaySubject(_bufferSize, _windowTime, _timestampProvider) {
		if (_bufferSize === void 0) _bufferSize = Infinity;
		if (_windowTime === void 0) _windowTime = Infinity;
		if (_timestampProvider === void 0) _timestampProvider = dateTimestampProvider;
		var _this = _super.call(this) || this;
		_this._bufferSize = _bufferSize;
		_this._windowTime = _windowTime;
		_this._timestampProvider = _timestampProvider;
		_this._buffer = [];
		_this._infiniteTimeWindow = true;
		_this._infiniteTimeWindow = _windowTime === Infinity;
		_this._bufferSize = Math.max(1, _bufferSize);
		_this._windowTime = Math.max(1, _windowTime);
		return _this;
	}
	ReplaySubject.prototype.next = function(value) {
		var _a = this, isStopped = _a.isStopped, _buffer = _a._buffer, _infiniteTimeWindow = _a._infiniteTimeWindow, _timestampProvider = _a._timestampProvider, _windowTime = _a._windowTime;
		if (!isStopped) {
			_buffer.push(value);
			!_infiniteTimeWindow && _buffer.push(_timestampProvider.now() + _windowTime);
		}
		this._trimBuffer();
		_super.prototype.next.call(this, value);
	};
	ReplaySubject.prototype._subscribe = function(subscriber) {
		this._throwIfClosed();
		this._trimBuffer();
		var subscription = this._innerSubscribe(subscriber);
		var _a = this, _infiniteTimeWindow = _a._infiniteTimeWindow;
		var copy = _a._buffer.slice();
		for (var i = 0; i < copy.length && !subscriber.closed; i += _infiniteTimeWindow ? 1 : 2) subscriber.next(copy[i]);
		this._checkFinalizedStatuses(subscriber);
		return subscription;
	};
	ReplaySubject.prototype._trimBuffer = function() {
		var _a = this, _bufferSize = _a._bufferSize, _timestampProvider = _a._timestampProvider, _buffer = _a._buffer, _infiniteTimeWindow = _a._infiniteTimeWindow;
		var adjustedBufferSize = (_infiniteTimeWindow ? 1 : 2) * _bufferSize;
		_bufferSize < Infinity && adjustedBufferSize < _buffer.length && _buffer.splice(0, _buffer.length - adjustedBufferSize);
		if (!_infiniteTimeWindow) {
			var now = _timestampProvider.now();
			var last = 0;
			for (var i = 1; i < _buffer.length && _buffer[i] <= now; i += 2) last = i;
			last && _buffer.splice(0, last + 1);
		}
	};
	return ReplaySubject;
}(Subject);
// node_modules/rxjs/dist/esm5/internal/observable/merge.js
function merge() {
	var args = [];
	for (var _i = 0; _i < arguments.length; _i++) args[_i] = arguments[_i];
	var scheduler = popScheduler(args);
	var concurrent = popNumber(args, Infinity);
	var sources = args;
	return !sources.length ? EMPTY$1 : sources.length === 1 ? innerFrom(sources[0]) : mergeAll(concurrent)(from(sources, scheduler));
}
// node_modules/rxjs/dist/esm5/internal/operators/pairwise.js
function pairwise() {
	return operate(function(source, subscriber) {
		var prev;
		var hasPrev = false;
		source.subscribe(createOperatorSubscriber(subscriber, function(value) {
			var p = prev;
			prev = value;
			hasPrev && subscriber.next([p, value]);
			hasPrev = true;
		}));
	});
}
// node_modules/rxjs/dist/esm5/internal/operators/shareReplay.js
function shareReplay(configOrBufferSize, windowTime, scheduler) {
	var _a, _b, _c;
	var bufferSize;
	var refCount = false;
	if (configOrBufferSize && typeof configOrBufferSize === "object") _a = configOrBufferSize.bufferSize, bufferSize = _a === void 0 ? Infinity : _a, _b = configOrBufferSize.windowTime, windowTime = _b === void 0 ? Infinity : _b, _c = configOrBufferSize.refCount, refCount = _c === void 0 ? false : _c, scheduler = configOrBufferSize.scheduler;
	else bufferSize = configOrBufferSize !== null && configOrBufferSize !== void 0 ? configOrBufferSize : Infinity;
	return share({
		connector: function() {
			return new ReplaySubject(bufferSize, windowTime, scheduler);
		},
		resetOnError: true,
		resetOnComplete: false,
		resetOnRefCountZero: refCount
	});
}
// node_modules/@angular/cdk/fesm2022/_typeahead-chunk.mjs
var DEFAULT_TYPEAHEAD_DEBOUNCE_INTERVAL_MS = 200;
var Typeahead = class {
	_letterKeyStream = new Subject();
	_items = [];
	_selectedItemIndex = -1;
	_pressedLetters = [];
	_skipPredicateFn;
	_selectedItem = new Subject();
	selectedItem = this._selectedItem;
	constructor(initialItems, config) {
		const typeAheadInterval = typeof config?.debounceInterval === "number" ? config.debounceInterval : DEFAULT_TYPEAHEAD_DEBOUNCE_INTERVAL_MS;
		if (config?.skipPredicate) this._skipPredicateFn = config.skipPredicate;
		if ((typeof ngDevMode === "undefined" || ngDevMode) && initialItems.length && initialItems.some((item) => typeof item.getLabel !== "function")) throw new Error("KeyManager items in typeahead mode must implement the `getLabel` method.");
		this.setItems(initialItems);
		this._setupKeyHandler(typeAheadInterval);
	}
	destroy() {
		this._pressedLetters = [];
		this._letterKeyStream.complete();
		this._selectedItem.complete();
	}
	setCurrentSelectedItemIndex(index) {
		this._selectedItemIndex = index;
	}
	setItems(items) {
		this._items = items;
	}
	handleKey(event) {
		const keyCode = event.keyCode;
		if (event.key && event.key.length === 1) this._letterKeyStream.next(event.key.toLocaleUpperCase());
		else if (keyCode >= 65 && keyCode <= 90 || keyCode >= 48 && keyCode <= 57) this._letterKeyStream.next(String.fromCharCode(keyCode));
	}
	isTyping() {
		return this._pressedLetters.length > 0;
	}
	reset() {
		this._pressedLetters = [];
	}
	_setupKeyHandler(typeAheadInterval) {
		this._letterKeyStream.pipe(tap((letter) => this._pressedLetters.push(letter)), debounceTime(typeAheadInterval), filter(() => this._pressedLetters.length > 0), map(() => this._pressedLetters.join("").toLocaleUpperCase())).subscribe((inputString) => {
			for (let i = 1; i < this._items.length + 1; i++) {
				const index = (this._selectedItemIndex + i) % this._items.length;
				const item = this._items[index];
				if (!this._skipPredicateFn?.(item) && item.getLabel?.().toLocaleUpperCase().trim().indexOf(inputString) === 0) {
					this._selectedItem.next(item);
					break;
				}
			}
			this._pressedLetters = [];
		});
	}
};
// node_modules/@angular/cdk/fesm2022/keycodes.mjs
function hasModifierKey(event, ...modifiers) {
	if (modifiers.length) return modifiers.some((modifier) => event[modifier]);
	return event.altKey || event.shiftKey || event.ctrlKey || event.metaKey;
}
// node_modules/@angular/cdk/fesm2022/_list-key-manager-chunk.mjs
var ListKeyManager = class {
	_items;
	_activeItemIndex = signal(-1, ...ngDevMode ? [{ debugName: "_activeItemIndex" }] : []);
	_activeItem = signal(null, ...ngDevMode ? [{ debugName: "_activeItem" }] : []);
	_wrap = false;
	_typeaheadSubscription = Subscription.EMPTY;
	_itemChangesSubscription;
	_vertical = true;
	_horizontal = null;
	_allowedModifierKeys = [];
	_homeAndEnd = false;
	_pageUpAndDown = {
		enabled: false,
		delta: 10
	};
	_effectRef;
	_typeahead;
	_skipPredicateFn = (item) => item.disabled;
	constructor(_items, injector) {
		this._items = _items;
		if (_items instanceof QueryList) this._itemChangesSubscription = _items.changes.subscribe((newItems) => this._itemsChanged(newItems.toArray()));
		else if (isSignal(_items)) {
			if (!injector && (typeof ngDevMode === "undefined" || ngDevMode)) throw new Error("ListKeyManager constructed with a signal must receive an injector");
			this._effectRef = effect(() => this._itemsChanged(_items()), {
				...ngDevMode ? { debugName: "_effectRef" } : {},
				injector
			});
		}
	}
	tabOut = new Subject();
	change = new Subject();
	skipPredicate(predicate) {
		this._skipPredicateFn = predicate;
		return this;
	}
	withWrap(shouldWrap = true) {
		this._wrap = shouldWrap;
		return this;
	}
	withVerticalOrientation(enabled = true) {
		this._vertical = enabled;
		return this;
	}
	withHorizontalOrientation(direction) {
		this._horizontal = direction;
		return this;
	}
	withAllowedModifierKeys(keys) {
		this._allowedModifierKeys = keys;
		return this;
	}
	withTypeAhead(debounceInterval = 200) {
		if (typeof ngDevMode === "undefined" || ngDevMode) {
			const items = this._getItemsArray();
			if (items.length > 0 && items.some((item) => typeof item.getLabel !== "function")) throw Error("ListKeyManager items in typeahead mode must implement the `getLabel` method.");
		}
		this._typeaheadSubscription.unsubscribe();
		const items = this._getItemsArray();
		this._typeahead = new Typeahead(items, {
			debounceInterval: typeof debounceInterval === "number" ? debounceInterval : void 0,
			skipPredicate: (item) => this._skipPredicateFn(item)
		});
		this._typeaheadSubscription = this._typeahead.selectedItem.subscribe((item) => {
			this.setActiveItem(item);
		});
		return this;
	}
	cancelTypeahead() {
		this._typeahead?.reset();
		return this;
	}
	withHomeAndEnd(enabled = true) {
		this._homeAndEnd = enabled;
		return this;
	}
	withPageUpDown(enabled = true, delta = 10) {
		this._pageUpAndDown = {
			enabled,
			delta
		};
		return this;
	}
	setActiveItem(item) {
		const previousActiveItem = this._activeItem();
		this.updateActiveItem(item);
		if (this._activeItem() !== previousActiveItem) this.change.next(this._activeItemIndex());
	}
	onKeydown(event) {
		const keyCode = event.keyCode;
		const isModifierAllowed = [
			"altKey",
			"ctrlKey",
			"metaKey",
			"shiftKey"
		].every((modifier) => {
			return !event[modifier] || this._allowedModifierKeys.indexOf(modifier) > -1;
		});
		switch (keyCode) {
			case 9:
				this.tabOut.next();
				return;
			case 40: if (this._vertical && isModifierAllowed) {
				this.setNextItemActive();
				break;
			} else return;
			case 38: if (this._vertical && isModifierAllowed) {
				this.setPreviousItemActive();
				break;
			} else return;
			case 39: if (this._horizontal && isModifierAllowed) {
				this._horizontal === "rtl" ? this.setPreviousItemActive() : this.setNextItemActive();
				break;
			} else return;
			case 37: if (this._horizontal && isModifierAllowed) {
				this._horizontal === "rtl" ? this.setNextItemActive() : this.setPreviousItemActive();
				break;
			} else return;
			case 36: if (this._homeAndEnd && isModifierAllowed) {
				this.setFirstItemActive();
				break;
			} else return;
			case 35: if (this._homeAndEnd && isModifierAllowed) {
				this.setLastItemActive();
				break;
			} else return;
			case 33: if (this._pageUpAndDown.enabled && isModifierAllowed) {
				const targetIndex = this._activeItemIndex() - this._pageUpAndDown.delta;
				this._setActiveItemByIndex(targetIndex > 0 ? targetIndex : 0, 1);
				break;
			} else return;
			case 34: if (this._pageUpAndDown.enabled && isModifierAllowed) {
				const targetIndex = this._activeItemIndex() + this._pageUpAndDown.delta;
				const itemsLength = this._getItemsArray().length;
				this._setActiveItemByIndex(targetIndex < itemsLength ? targetIndex : itemsLength - 1, -1);
				break;
			} else return;
			default:
				if (isModifierAllowed || hasModifierKey(event, "shiftKey")) this._typeahead?.handleKey(event);
				return;
		}
		this._typeahead?.reset();
		event.preventDefault();
	}
	get activeItemIndex() {
		return this._activeItemIndex();
	}
	get activeItem() {
		return this._activeItem();
	}
	isTyping() {
		return !!this._typeahead && this._typeahead.isTyping();
	}
	setFirstItemActive() {
		this._setActiveItemByIndex(0, 1);
	}
	setLastItemActive() {
		this._setActiveItemByIndex(this._getItemsArray().length - 1, -1);
	}
	setNextItemActive() {
		this._activeItemIndex() < 0 ? this.setFirstItemActive() : this._setActiveItemByDelta(1);
	}
	setPreviousItemActive() {
		this._activeItemIndex() < 0 && this._wrap ? this.setLastItemActive() : this._setActiveItemByDelta(-1);
	}
	updateActiveItem(item) {
		const itemArray = this._getItemsArray();
		const index = typeof item === "number" ? item : itemArray.indexOf(item);
		const activeItem = itemArray[index];
		this._activeItem.set(activeItem == null ? null : activeItem);
		this._activeItemIndex.set(index);
		this._typeahead?.setCurrentSelectedItemIndex(index);
	}
	destroy() {
		this._typeaheadSubscription.unsubscribe();
		this._itemChangesSubscription?.unsubscribe();
		this._effectRef?.destroy();
		this._typeahead?.destroy();
		this.tabOut.complete();
		this.change.complete();
	}
	_setActiveItemByDelta(delta) {
		this._wrap ? this._setActiveInWrapMode(delta) : this._setActiveInDefaultMode(delta);
	}
	_setActiveInWrapMode(delta) {
		const items = this._getItemsArray();
		for (let i = 1; i <= items.length; i++) {
			const index = (this._activeItemIndex() + delta * i + items.length) % items.length;
			const item = items[index];
			if (!this._skipPredicateFn(item)) {
				this.setActiveItem(index);
				return;
			}
		}
	}
	_setActiveInDefaultMode(delta) {
		this._setActiveItemByIndex(this._activeItemIndex() + delta, delta);
	}
	_setActiveItemByIndex(index, fallbackDelta) {
		const items = this._getItemsArray();
		if (!items[index]) return;
		while (this._skipPredicateFn(items[index])) {
			index += fallbackDelta;
			if (!items[index]) return;
		}
		this.setActiveItem(index);
	}
	_getItemsArray() {
		if (isSignal(this._items)) return this._items();
		return this._items instanceof QueryList ? this._items.toArray() : this._items;
	}
	_itemsChanged(newItems) {
		this._typeahead?.setItems(newItems);
		const activeItem = this._activeItem();
		if (activeItem) {
			const newIndex = newItems.indexOf(activeItem);
			if (newIndex > -1 && newIndex !== this._activeItemIndex()) {
				this._activeItemIndex.set(newIndex);
				this._typeahead?.setCurrentSelectedItemIndex(newIndex);
			}
		}
	}
};
// node_modules/@angular/cdk/fesm2022/_focus-key-manager-chunk.mjs
var FocusKeyManager = class extends ListKeyManager {
	_origin = "program";
	setFocusOrigin(origin) {
		this._origin = origin;
		return this;
	}
	setActiveItem(item) {
		super.setActiveItem(item);
		if (this.activeItem) this.activeItem.focus(this._origin);
	}
};
// node_modules/@angular/cdk/fesm2022/_id-generator-chunk.mjs
var counters = /* @__PURE__ */ new Map();
var _IdGenerator = class _IdGenerator {
	_appId = inject(APP_ID);
	static _infix = `a${Math.floor(Math.random() * 1e5).toString()}`;
	getId(prefix, randomize = false) {
		if (this._appId !== "ng") prefix += this._appId;
		let count = counters.get(prefix);
		if (count === void 0) count = 0;
		else count++;
		counters.set(prefix, count);
		return `${prefix}${randomize ? _IdGenerator._infix + "-" : ""}${count}`;
	}
	static ɵfac = function _IdGenerator_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || _IdGenerator)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: _IdGenerator,
		factory: _IdGenerator.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(_IdGenerator, [{ type: Service }], null, null);
})();
// node_modules/@angular/cdk/fesm2022/coercion.mjs
function coerceBooleanProperty(value) {
	return value != null && `${value}` !== "false";
}
// node_modules/@angular/forms/fesm2022/forms.mjs
/**
* @license Angular v22.2.4
* (c) 2010-2026 Google LLC. https://angular.dev/
* License: MIT
*/
var BaseControlValueAccessor = class BaseControlValueAccessor {
	_renderer;
	_elementRef;
	onChange = (_) => {};
	onTouched = () => {};
	constructor(_renderer, _elementRef) {
		this._renderer = _renderer;
		this._elementRef = _elementRef;
	}
	setProperty(key, value) {
		this._renderer.setProperty(this._elementRef.nativeElement, key, value);
	}
	registerOnTouched(fn) {
		this.onTouched = fn;
	}
	registerOnChange(fn) {
		this.onChange = fn;
	}
	setDisabledState(isDisabled) {
		this.setProperty("disabled", isDisabled);
	}
	static ɵfac = function BaseControlValueAccessor_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || BaseControlValueAccessor)(ɵɵdirectiveInject(Renderer2), ɵɵdirectiveInject(ElementRef));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({ type: BaseControlValueAccessor });
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(BaseControlValueAccessor, [{ type: Directive }], () => [{ type: Renderer2 }, { type: ElementRef }], null);
})();
var BuiltInControlValueAccessor = class BuiltInControlValueAccessor extends BaseControlValueAccessor {
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵBuiltInControlValueAccessor_BaseFactory = void 0;
		return function BuiltInControlValueAccessor_Factory(__ngFactoryType__) {
			return (ɵBuiltInControlValueAccessor_BaseFactory || (ɵBuiltInControlValueAccessor_BaseFactory = ɵɵgetInheritedFactory(BuiltInControlValueAccessor)))(__ngFactoryType__ || BuiltInControlValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: BuiltInControlValueAccessor,
		features: [ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(BuiltInControlValueAccessor, [{ type: Directive }], null, null);
})();
var NG_VALUE_ACCESSOR = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "NgValueAccessor" : "");
var CHECKBOX_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => CheckboxControlValueAccessor),
	multi: true
};
var CheckboxControlValueAccessor = class CheckboxControlValueAccessor extends BuiltInControlValueAccessor {
	writeValue(value) {
		this.setProperty("checked", value);
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵCheckboxControlValueAccessor_BaseFactory = void 0;
		return function CheckboxControlValueAccessor_Factory(__ngFactoryType__) {
			return (ɵCheckboxControlValueAccessor_BaseFactory || (ɵCheckboxControlValueAccessor_BaseFactory = ɵɵgetInheritedFactory(CheckboxControlValueAccessor)))(__ngFactoryType__ || CheckboxControlValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: CheckboxControlValueAccessor,
		selectors: [
			[
				"input",
				"type",
				"checkbox",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"checkbox",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"checkbox",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function CheckboxControlValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("change", function CheckboxControlValueAccessor_change_HostBindingHandler($event) {
				return ctx.onChange($event.target.checked);
			})("blur", function CheckboxControlValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		standalone: false,
		features: [ɵɵProvidersFeature([CHECKBOX_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(CheckboxControlValueAccessor, [{
		type: Directive,
		args: [{
			selector: "input[type=checkbox]:not([ngNoCva])[formControlName],input[type=checkbox]:not([ngNoCva])[formControl],input[type=checkbox]:not([ngNoCva])[ngModel]",
			host: {
				"(change)": "onChange($any($event.target).checked)",
				"(blur)": "onTouched()"
			},
			providers: [CHECKBOX_VALUE_ACCESSOR],
			standalone: false
		}]
	}], null, null);
})();
var DEFAULT_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => DefaultValueAccessor),
	multi: true
};
function _isAndroid() {
	const userAgent = getDOM() ? getDOM().getUserAgent() : "";
	return /android (\d+)/.test(userAgent.toLowerCase());
}
var COMPOSITION_BUFFER_MODE = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "CompositionEventMode" : "");
var DefaultValueAccessor = class DefaultValueAccessor extends BaseControlValueAccessor {
	_compositionMode;
	_composing = false;
	constructor(renderer, elementRef, _compositionMode) {
		super(renderer, elementRef);
		this._compositionMode = _compositionMode;
		if (this._compositionMode == null) this._compositionMode = !_isAndroid();
	}
	writeValue(value) {
		const normalizedValue = value == null ? "" : value;
		this.setProperty("value", normalizedValue);
	}
	_handleInput(value) {
		if (!this._compositionMode || this._compositionMode && !this._composing) this.onChange(value);
	}
	_compositionStart() {
		this._composing = true;
	}
	_compositionEnd(value) {
		this._composing = false;
		this._compositionMode && this.onChange(value);
	}
	static ɵfac = function DefaultValueAccessor_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || DefaultValueAccessor)(ɵɵdirectiveInject(Renderer2), ɵɵdirectiveInject(ElementRef), ɵɵdirectiveInject(COMPOSITION_BUFFER_MODE, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: DefaultValueAccessor,
		selectors: [
			[
				"input",
				"formControlName",
				"",
				3,
				"type",
				"checkbox",
				3,
				"ngNoCva",
				""
			],
			[
				"textarea",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"formControl",
				"",
				3,
				"type",
				"checkbox",
				3,
				"ngNoCva",
				""
			],
			[
				"textarea",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"ngModel",
				"",
				3,
				"type",
				"checkbox",
				3,
				"ngNoCva",
				""
			],
			[
				"textarea",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"",
				"ngDefaultControl",
				""
			]
		],
		hostBindings: function DefaultValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("input", function DefaultValueAccessor_input_HostBindingHandler($event) {
				return ctx._handleInput($event.target.value);
			})("blur", function DefaultValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			})("compositionstart", function DefaultValueAccessor_compositionstart_HostBindingHandler() {
				return ctx._compositionStart();
			})("compositionend", function DefaultValueAccessor_compositionend_HostBindingHandler($event) {
				return ctx._compositionEnd($event.target.value);
			});
		},
		standalone: false,
		features: [ɵɵProvidersFeature([DEFAULT_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(DefaultValueAccessor, [{
		type: Directive,
		args: [{
			selector: "input:not([type=checkbox]):not([ngNoCva])[formControlName],textarea:not([ngNoCva])[formControlName],input:not([type=checkbox]):not([ngNoCva])[formControl],textarea:not([ngNoCva])[formControl],input:not([type=checkbox]):not([ngNoCva])[ngModel],textarea:not([ngNoCva])[ngModel],[ngDefaultControl]",
			host: {
				"(input)": "_handleInput($any($event.target).value)",
				"(blur)": "onTouched()",
				"(compositionstart)": "_compositionStart()",
				"(compositionend)": "_compositionEnd($any($event.target).value)"
			},
			providers: [DEFAULT_VALUE_ACCESSOR],
			standalone: false
		}]
	}], () => [
		{ type: Renderer2 },
		{ type: ElementRef },
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [COMPOSITION_BUFFER_MODE]
			}]
		}
	], null);
})();
function isEmptyInputValue(value) {
	return value == null || lengthOrSize(value) === 0;
}
function lengthOrSize(value) {
	if (value == null) return null;
	else if (Array.isArray(value) || typeof value === "string") return value.length;
	else if (value instanceof Set) return value.size;
	return null;
}
var NG_VALIDATORS = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "NgValidators" : "");
var NG_ASYNC_VALIDATORS = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "NgAsyncValidators" : "");
var EMAIL_REGEXP = /^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
var Validators = class {
	static min(min) {
		return minValidator(min);
	}
	static max(max) {
		return maxValidator(max);
	}
	static required(control) {
		return requiredValidator(control);
	}
	static requiredTrue(control) {
		return requiredTrueValidator(control);
	}
	static email(control) {
		return emailValidator(control);
	}
	static minLength(minLength) {
		return minLengthValidator(minLength);
	}
	static maxLength(maxLength) {
		return maxLengthValidator(maxLength);
	}
	static pattern(pattern) {
		return patternValidator(pattern);
	}
	static nullValidator(control) {
		return nullValidator();
	}
	static compose(validators) {
		return compose(validators);
	}
	static composeAsync(validators) {
		return composeAsync(validators);
	}
};
function minValidator(min) {
	return (control) => {
		if (control.value == null || min == null) return null;
		const value = parseFloat(control.value);
		return !isNaN(value) && value < min ? { "min": {
			"min": min,
			"actual": control.value
		} } : null;
	};
}
function maxValidator(max) {
	return (control) => {
		if (control.value == null || max == null) return null;
		const value = parseFloat(control.value);
		return !isNaN(value) && value > max ? { "max": {
			"max": max,
			"actual": control.value
		} } : null;
	};
}
function requiredValidator(control) {
	return isEmptyInputValue(control.value) ? { "required": true } : null;
}
function requiredTrueValidator(control) {
	return control.value === true ? null : { "required": true };
}
function emailValidator(control) {
	if (isEmptyInputValue(control.value)) return null;
	return EMAIL_REGEXP.test(control.value) ? null : { "email": true };
}
function minLengthValidator(minLength) {
	return (control) => {
		const length = control.value?.length ?? lengthOrSize(control.value);
		if (length === null || length === 0) return null;
		return length < minLength ? { "minlength": {
			"requiredLength": minLength,
			"actualLength": length
		} } : null;
	};
}
function maxLengthValidator(maxLength) {
	return (control) => {
		const length = control.value?.length ?? lengthOrSize(control.value);
		if (length !== null && length > maxLength) return { "maxlength": {
			"requiredLength": maxLength,
			"actualLength": length
		} };
		return null;
	};
}
function patternValidator(pattern) {
	if (!pattern) return nullValidator;
	let regex;
	let regexStr;
	if (typeof pattern === "string") {
		regexStr = "";
		if (pattern.charAt(0) !== "^") regexStr += "^";
		regexStr += pattern;
		if (pattern.charAt(pattern.length - 1) !== "$") regexStr += "$";
		regex = new RegExp(regexStr);
	} else {
		regexStr = pattern.toString();
		regex = pattern;
	}
	return (control) => {
		if (isEmptyInputValue(control.value)) return null;
		const value = control.value;
		return regex.test(value) ? null : { "pattern": {
			"requiredPattern": regexStr,
			"actualValue": value
		} };
	};
}
function nullValidator(control) {
	return null;
}
function isPresent(o) {
	return o != null;
}
function toObservable(value) {
	const obs = isPromise(value) ? from(value) : value;
	if ((typeof ngDevMode === "undefined" || ngDevMode) && !isSubscribable(obs)) {
		let errorMessage = `Expected async validator to return Promise or Observable.`;
		if (typeof value === "object") errorMessage += " Are you using a synchronous validator where an async validator is expected?";
		throw new RuntimeError(-1101, errorMessage);
	}
	return obs;
}
function mergeErrors(arrayOfErrors) {
	let res = {};
	arrayOfErrors.forEach((errors) => {
		res = errors != null ? {
			...res,
			...errors
		} : res;
	});
	return Object.keys(res).length === 0 ? null : res;
}
function executeValidators(control, validators) {
	return validators.map((validator) => validator(control));
}
function isValidatorFn(validator) {
	return !validator.validate;
}
function normalizeValidators(validators) {
	return validators.map((validator) => {
		return isValidatorFn(validator) ? validator : (c) => validator.validate(c);
	});
}
function compose(validators) {
	if (!validators) return null;
	const presentValidators = validators.filter(isPresent);
	if (presentValidators.length == 0) return null;
	return function(control) {
		return mergeErrors(executeValidators(control, presentValidators));
	};
}
function composeValidators(validators) {
	return validators != null ? compose(normalizeValidators(validators)) : null;
}
function composeAsync(validators) {
	if (!validators) return null;
	const presentValidators = validators.filter(isPresent);
	if (presentValidators.length == 0) return null;
	return function(control) {
		const observables = executeValidators(control, presentValidators).map(toObservable);
		return forkJoin(observables).pipe(map(mergeErrors));
	};
}
function composeAsyncValidators(validators) {
	return validators != null ? composeAsync(normalizeValidators(validators)) : null;
}
function mergeValidators(controlValidators, dirValidator) {
	if (controlValidators === null) return [dirValidator];
	return Array.isArray(controlValidators) ? [...controlValidators, dirValidator] : [controlValidators, dirValidator];
}
function getControlValidators(control) {
	return control._rawValidators;
}
function getControlAsyncValidators(control) {
	return control._rawAsyncValidators;
}
function makeValidatorsArray(validators) {
	if (!validators) return [];
	return Array.isArray(validators) ? validators : [validators];
}
function hasValidator(validators, validator) {
	return Array.isArray(validators) ? validators.includes(validator) : validators === validator;
}
function addValidators(validators, currentValidators) {
	const current = makeValidatorsArray(currentValidators);
	makeValidatorsArray(validators).forEach((v) => {
		if (!hasValidator(current, v)) current.push(v);
	});
	return current;
}
function removeValidators(validators, currentValidators) {
	return makeValidatorsArray(currentValidators).filter((v) => !hasValidator(validators, v));
}
var AbstractControlDirective = class {
	get value() {
		return this.control ? this.control.value : null;
	}
	get valid() {
		return this.control ? this.control.valid : null;
	}
	get invalid() {
		return this.control ? this.control.invalid : null;
	}
	get pending() {
		return this.control ? this.control.pending : null;
	}
	get disabled() {
		return this.control ? this.control.disabled : null;
	}
	get enabled() {
		return this.control ? this.control.enabled : null;
	}
	get errors() {
		return this.control ? this.control.errors : null;
	}
	get pristine() {
		return this.control ? this.control.pristine : null;
	}
	get dirty() {
		return this.control ? this.control.dirty : null;
	}
	get touched() {
		return this.control ? this.control.touched : null;
	}
	get status() {
		return this.control ? this.control.status : null;
	}
	get untouched() {
		return this.control ? this.control.untouched : null;
	}
	get statusChanges() {
		return this.control ? this.control.statusChanges : null;
	}
	get valueChanges() {
		return this.control ? this.control.valueChanges : null;
	}
	get path() {
		return null;
	}
	_composedValidatorFn;
	_composedAsyncValidatorFn;
	_rawValidators = [];
	_rawAsyncValidators = [];
	_setValidators(validators) {
		this._rawValidators = validators || [];
		this._composedValidatorFn = composeValidators(this._rawValidators);
	}
	_setAsyncValidators(validators) {
		this._rawAsyncValidators = validators || [];
		this._composedAsyncValidatorFn = composeAsyncValidators(this._rawAsyncValidators);
	}
	get validator() {
		return this._composedValidatorFn || null;
	}
	get asyncValidator() {
		return this._composedAsyncValidatorFn || null;
	}
	_onDestroyCallbacks = [];
	_registerOnDestroy(fn) {
		this._onDestroyCallbacks.push(fn);
	}
	_invokeOnDestroyCallbacks() {
		this._onDestroyCallbacks.forEach((fn) => fn());
		this._onDestroyCallbacks = [];
	}
	reset(value = void 0) {
		this.control?.reset(value);
	}
	hasError(errorCode, path) {
		return this.control ? this.control.hasError(errorCode, path) : false;
	}
	getError(errorCode, path) {
		return this.control ? this.control.getError(errorCode, path) : null;
	}
};
var ControlContainer = class extends AbstractControlDirective {
	name;
	get formDirective() {
		return null;
	}
	get path() {
		return null;
	}
};
var formControlNameExample = `
  <div [formGroup]="myGroup">
    <input formControlName="firstName">
  </div>

  In your class:

  this.myGroup = new FormGroup({
      firstName: new FormControl()
  });`;
var formGroupNameExample = `
  <div [formGroup]="myGroup">
      <div formGroupName="person">
        <input formControlName="firstName">
      </div>
  </div>

  In your class:

  this.myGroup = new FormGroup({
      person: new FormGroup({ firstName: new FormControl() })
  });`;
var formArrayNameExample = `
  <div [formGroup]="myGroup">
    <div formArrayName="cities">
      <div *ngFor="let city of cityArray.controls; index as i">
        <input [formControlName]="i">
      </div>
    </div>
  </div>

  In your class:

  this.cityArray = new FormArray([new FormControl('SF')]);
  this.myGroup = new FormGroup({
    cities: this.cityArray
  });`;
var ngModelGroupExample = `
  <form>
      <div ngModelGroup="person">
        <input [(ngModel)]="person.name" name="firstName">
      </div>
  </form>`;
var ngModelWithFormGroupExample = `
  <div [formGroup]="myGroup">
      <input formControlName="firstName">
      <input [(ngModel)]="showMoreControls" [ngModelOptions]="{standalone: true}">
  </div>
`;
var VERSION = /* @__PURE__ */ new Version("22.2.4");
function controlParentException(nameOrIndex) {
	return new RuntimeError(1050, `formControlName must be used with a parent formGroup or formArray directive. You'll want to add a formGroup/formArray
      directive and pass it an existing FormGroup/FormArray instance (you can create one in your class).

      ${describeFormControl(nameOrIndex)}

    Example:

    ${formControlNameExample}`);
}
function describeFormControl(nameOrIndex) {
	if (nameOrIndex == null || nameOrIndex === "") return "";
	return `Affected Form Control ${typeof nameOrIndex === "string" ? "name" : "index"}: "${nameOrIndex}"`;
}
function ngModelGroupException() {
	return new RuntimeError(1051, `formControlName cannot be used with an ngModelGroup parent. It is only compatible with parents
      that also have a "form" prefix: formGroupName, formArrayName, or formGroup.

      Option 1:  Update the parent to be formGroupName (reactive form strategy)

      ${formGroupNameExample}

      Option 2: Use ngModel instead of formControlName (template-driven strategy)

      ${ngModelGroupExample}`);
}
function groupParentException() {
	return new RuntimeError(1053, `formGroupName must be used with a parent formGroup directive.  You'll want to add a formGroup
    directive and pass it an existing FormGroup instance (you can create one in your class).

    Example:

    ${formGroupNameExample}`);
}
function arrayParentException() {
	return new RuntimeError(1054, `formArrayName must be used with a parent formGroup directive.  You'll want to add a formGroup
      directive and pass it an existing FormGroup instance (you can create one in your class).

      Example:

      ${formArrayNameExample}`);
}
var disabledAttrWarning = `
  It looks like you're using the disabled attribute with a reactive form directive. If you set disabled to true
  when you set up this control in your component class, the disabled attribute will actually be set in the DOM for
  you. We recommend using this approach to avoid 'changed after checked' errors.

  Example:
  // Specify the \`disabled\` property at control creation time:
  form = new FormGroup({
    first: new FormControl({value: 'Nancy', disabled: true}, Validators.required),
    last: new FormControl('Drew', Validators.required)
  });

  // Controls can also be enabled/disabled after creation:
  form.get('first')?.enable();
  form.get('last')?.disable();
`;
var asyncValidatorsDroppedWithOptsWarning = `
  It looks like you're constructing using a FormControl with both an options argument and an
  async validators argument. Mixing these arguments will cause your async validators to be dropped.
  You should either put all your validators in the options object, or in separate validators
  arguments. For example:

  // Using validators arguments
  fc = new FormControl(42, Validators.required, myAsyncValidator);

  // Using AbstractControlOptions
  fc = new FormControl(42, {validators: Validators.required, asyncValidators: myAV});

  // Do NOT mix them: async validators will be dropped!
  fc = new FormControl(42, {validators: Validators.required}, /* Oops! */ myAsyncValidator);
`;
function ngModelWarning(directiveName) {
	return `
  It looks like you're using ngModel on the same form field as ${directiveName}.
  Support for using the ngModel input property and ngModelChange event with
  reactive form directives has been deprecated in Angular v6 and will be removed
  in a future version of Angular.

  For more information on this, see our API docs here:
  https://${VERSION.major !== "0" ? `v${VERSION.major}.` : ""}angular.dev/api/forms/${directiveName === "formControl" ? "FormControlDirective" : "FormControlName"}
  `;
}
function describeKey(isFormGroup, key) {
	return isFormGroup ? `with name: '${key}'` : `at index: ${key}`;
}
function noControlsError(isFormGroup) {
	return `
    There are no form controls registered with this ${isFormGroup ? "group" : "array"} yet. If you're using ngModel,
    you may want to check next tick (e.g. use setTimeout).
  `;
}
function missingControlError(isFormGroup, key) {
	return `Cannot find form control ${describeKey(isFormGroup, key)}`;
}
function missingControlValueError(isFormGroup, key) {
	return `Must supply a value for form control ${describeKey(isFormGroup, key)}`;
}
function missingControlForPathError(form, path) {
	const parent = path.length > 1 ? form.get(path.slice(0, -1)) : form;
	const controls = parent && parent.controls;
	const siblingKeys = controls ? Object.keys(controls) : [];
	const siblings = siblingKeys.length ? `Controls that do exist at this level: ${siblingKeys.join(", ")}.` : `No controls are registered at this level.`;
	return `Cannot find control with path: '${path.join(" -> ")}'. ${siblings}`;
}
var VALID = "VALID";
var INVALID = "INVALID";
var PENDING = "PENDING";
var DISABLED = "DISABLED";
var ControlEvent = class {};
var ValueChangeEvent = class extends ControlEvent {
	value;
	source;
	constructor(value, source) {
		super();
		this.value = value;
		this.source = source;
	}
};
var PristineChangeEvent = class extends ControlEvent {
	pristine;
	source;
	constructor(pristine, source) {
		super();
		this.pristine = pristine;
		this.source = source;
	}
};
var TouchedChangeEvent = class extends ControlEvent {
	touched;
	source;
	constructor(touched, source) {
		super();
		this.touched = touched;
		this.source = source;
	}
};
var StatusChangeEvent = class extends ControlEvent {
	status;
	source;
	constructor(status, source) {
		super();
		this.status = status;
		this.source = source;
	}
};
var FormSubmittedEvent = class extends ControlEvent {
	source;
	constructor(source) {
		super();
		this.source = source;
	}
};
var FormResetEvent = class extends ControlEvent {
	source;
	constructor(source) {
		super();
		this.source = source;
	}
};
function pickValidators(validatorOrOpts) {
	return (isOptionsObj(validatorOrOpts) ? validatorOrOpts.validators : validatorOrOpts) || null;
}
function coerceToValidator(validator) {
	return Array.isArray(validator) ? composeValidators(validator) : validator || null;
}
function pickAsyncValidators(asyncValidator, validatorOrOpts) {
	if (typeof ngDevMode === "undefined" || ngDevMode) {
		if (isOptionsObj(validatorOrOpts) && asyncValidator) console.warn(asyncValidatorsDroppedWithOptsWarning);
	}
	return (isOptionsObj(validatorOrOpts) ? validatorOrOpts.asyncValidators : asyncValidator) || null;
}
function coerceToAsyncValidator(asyncValidator) {
	return Array.isArray(asyncValidator) ? composeAsyncValidators(asyncValidator) : asyncValidator || null;
}
function isOptionsObj(validatorOrOpts) {
	return validatorOrOpts != null && !Array.isArray(validatorOrOpts) && typeof validatorOrOpts === "object";
}
function assertControlPresent(parent, isGroup, key) {
	const controls = parent.controls;
	if (!(isGroup ? Object.keys(controls) : controls).length) throw new RuntimeError(1e3, typeof ngDevMode === "undefined" || ngDevMode ? noControlsError(isGroup) : "");
	if (!hasOwnControl(controls, key)) throw new RuntimeError(1001, typeof ngDevMode === "undefined" || ngDevMode ? missingControlError(isGroup, key) : "");
}
function assertAllValuesPresent(control, isGroup, value) {
	control._forEachChild((_, key) => {
		if (value[key] === void 0) throw new RuntimeError(-1002, typeof ngDevMode === "undefined" || ngDevMode ? missingControlValueError(isGroup, key) : "");
	});
}
var AbstractControl = class {
	_pendingDirty = false;
	_hasOwnPendingAsyncValidator = null;
	_pendingTouched = false;
	_onCollectionChange = () => {};
	_updateOn;
	_hasRequired = signal(false, ...ngDevMode ? [{ debugName: "_hasRequired" }] : []);
	_parent = null;
	_asyncValidationSubscription;
	_composedValidatorFn;
	_composedAsyncValidatorFn;
	_rawValidators;
	_rawAsyncValidators;
	value;
	constructor(validators, asyncValidators) {
		this._assignValidators(validators);
		this._assignAsyncValidators(asyncValidators);
	}
	get validator() {
		return this._composedValidatorFn;
	}
	set validator(validatorFn) {
		this._rawValidators = this._composedValidatorFn = validatorFn;
		this._updateHasRequiredValidator();
	}
	get asyncValidator() {
		return this._composedAsyncValidatorFn;
	}
	set asyncValidator(asyncValidatorFn) {
		this._rawAsyncValidators = this._composedAsyncValidatorFn = asyncValidatorFn;
	}
	get parent() {
		return this._parent;
	}
	get status() {
		return untracked(this.statusReactive);
	}
	set status(v) {
		untracked(() => this.statusReactive.set(v));
	}
	_status = computed(() => this.statusReactive(), ...ngDevMode ? [{ debugName: "_status" }] : []);
	statusReactive = signal(void 0, ...ngDevMode ? [{ debugName: "statusReactive" }] : []);
	get valid() {
		return this.status === VALID;
	}
	get invalid() {
		return this.status === INVALID;
	}
	get pending() {
		return this.status === PENDING;
	}
	get disabled() {
		return this.status === DISABLED;
	}
	get enabled() {
		return this.status !== DISABLED;
	}
	errors;
	get pristine() {
		return untracked(this.pristineReactive);
	}
	set pristine(v) {
		untracked(() => this.pristineReactive.set(v));
	}
	_pristine = computed(() => this.pristineReactive(), ...ngDevMode ? [{ debugName: "_pristine" }] : []);
	pristineReactive = signal(true, ...ngDevMode ? [{ debugName: "pristineReactive" }] : []);
	get dirty() {
		return !this.pristine;
	}
	get touched() {
		return untracked(this.touchedReactive);
	}
	set touched(v) {
		untracked(() => this.touchedReactive.set(v));
	}
	_touched = computed(() => this.touchedReactive(), ...ngDevMode ? [{ debugName: "_touched" }] : []);
	touchedReactive = signal(false, ...ngDevMode ? [{ debugName: "touchedReactive" }] : []);
	get untouched() {
		return !this.touched;
	}
	_events = new Subject();
	events = this._events.asObservable();
	valueChanges;
	statusChanges;
	get updateOn() {
		return this._updateOn ? this._updateOn : this.parent ? this.parent.updateOn : "change";
	}
	setValidators(validators) {
		this._assignValidators(validators);
	}
	setAsyncValidators(validators) {
		this._assignAsyncValidators(validators);
	}
	addValidators(validators) {
		this.setValidators(addValidators(validators, this._rawValidators));
	}
	addAsyncValidators(validators) {
		this.setAsyncValidators(addValidators(validators, this._rawAsyncValidators));
	}
	removeValidators(validators) {
		this.setValidators(removeValidators(validators, this._rawValidators));
	}
	removeAsyncValidators(validators) {
		this.setAsyncValidators(removeValidators(validators, this._rawAsyncValidators));
	}
	hasValidator(validator) {
		return hasValidator(this._rawValidators, validator);
	}
	hasAsyncValidator(validator) {
		return hasValidator(this._rawAsyncValidators, validator);
	}
	clearValidators() {
		this.validator = null;
	}
	clearAsyncValidators() {
		this.asyncValidator = null;
	}
	markAsTouched(opts = {}) {
		const changed = this.touched === false;
		this.touched = true;
		const sourceControl = opts.sourceControl ?? this;
		if (!opts.onlySelf) this._parent?.markAsTouched({
			...opts,
			sourceControl
		});
		if (changed && opts.emitEvent !== false) this._events.next(new TouchedChangeEvent(true, sourceControl));
	}
	markAllAsDirty(opts = {}) {
		this.markAsDirty({
			onlySelf: true,
			emitEvent: opts.emitEvent,
			sourceControl: this
		});
		this._forEachChild((control) => control.markAllAsDirty(opts));
	}
	markAllAsTouched(opts = {}) {
		this.markAsTouched({
			onlySelf: true,
			emitEvent: opts.emitEvent,
			sourceControl: this
		});
		this._forEachChild((control) => control.markAllAsTouched(opts));
	}
	markAsUntouched(opts = {}) {
		const changed = this.touched === true;
		this.touched = false;
		this._pendingTouched = false;
		const sourceControl = opts.sourceControl ?? this;
		this._forEachChild((control) => {
			control.markAsUntouched({
				onlySelf: true,
				emitEvent: opts.emitEvent,
				sourceControl
			});
		});
		if (!opts.onlySelf) this._parent?._updateTouched(opts, sourceControl);
		if (changed && opts.emitEvent !== false) this._events.next(new TouchedChangeEvent(false, sourceControl));
	}
	markAsDirty(opts = {}) {
		const changed = this.pristine === true;
		this.pristine = false;
		const sourceControl = opts.sourceControl ?? this;
		if (!opts.onlySelf) this._parent?.markAsDirty({
			...opts,
			sourceControl
		});
		if (changed && opts.emitEvent !== false) this._events.next(new PristineChangeEvent(false, sourceControl));
	}
	markAsPristine(opts = {}) {
		const changed = this.pristine === false;
		this.pristine = true;
		this._pendingDirty = false;
		const sourceControl = opts.sourceControl ?? this;
		this._forEachChild((control) => {
			control.markAsPristine({
				onlySelf: true,
				emitEvent: opts.emitEvent
			});
		});
		if (!opts.onlySelf) this._parent?._updatePristine(opts, sourceControl);
		if (changed && opts.emitEvent !== false) this._events.next(new PristineChangeEvent(true, sourceControl));
	}
	markAsPending(opts = {}) {
		this.status = PENDING;
		const sourceControl = opts.sourceControl ?? this;
		if (opts.emitEvent !== false) {
			this._events.next(new StatusChangeEvent(this.status, sourceControl));
			this.statusChanges.emit(this.status);
		}
		if (!opts.onlySelf) this._parent?.markAsPending({
			...opts,
			sourceControl
		});
	}
	disable(opts = {}) {
		const skipPristineCheck = this._parentMarkedDirty(opts.onlySelf);
		this.status = DISABLED;
		this.errors = null;
		this._forEachChild((control) => {
			control.disable({
				...opts,
				onlySelf: true
			});
		});
		this._updateValue();
		const sourceControl = opts.sourceControl ?? this;
		if (opts.emitEvent !== false) {
			this._events.next(new ValueChangeEvent(this.value, sourceControl));
			this._events.next(new StatusChangeEvent(this.status, sourceControl));
			this.valueChanges.emit(this.value);
			this.statusChanges.emit(this.status);
		}
		this._updateAncestors({
			...opts,
			skipPristineCheck
		}, this);
		this._onDisabledChange.forEach((changeFn) => changeFn(true));
	}
	enable(opts = {}) {
		const skipPristineCheck = this._parentMarkedDirty(opts.onlySelf);
		this.status = VALID;
		this._forEachChild((control) => {
			control.enable({
				...opts,
				onlySelf: true
			});
		});
		this.updateValueAndValidity({
			onlySelf: true,
			emitEvent: opts.emitEvent
		});
		this._updateAncestors({
			...opts,
			skipPristineCheck
		}, this);
		this._onDisabledChange.forEach((changeFn) => changeFn(false));
	}
	_updateAncestors(opts, sourceControl) {
		if (!opts.onlySelf) {
			this._parent?.updateValueAndValidity(opts);
			if (!opts.skipPristineCheck) this._parent?._updatePristine({}, sourceControl);
			this._parent?._updateTouched({}, sourceControl);
		}
	}
	setParent(parent) {
		this._parent = parent;
	}
	getRawValue() {
		return this.value;
	}
	updateValueAndValidity(opts = {}) {
		this._setInitialStatus();
		this._updateValue();
		if (this.enabled) {
			const shouldHaveEmitted = this._cancelExistingSubscription();
			this.errors = this._runValidator();
			this.status = this._calculateStatus();
			if (this.status === VALID || this.status === PENDING) this._runAsyncValidator(shouldHaveEmitted, opts.emitEvent);
		}
		const sourceControl = opts.sourceControl ?? this;
		if (opts.emitEvent !== false) {
			this._events.next(new ValueChangeEvent(this.value, sourceControl));
			this._events.next(new StatusChangeEvent(this.status, sourceControl));
			this.valueChanges.emit(this.value);
			this.statusChanges.emit(this.status);
		}
		if (!opts.onlySelf) this._parent?.updateValueAndValidity({
			...opts,
			sourceControl
		});
	}
	_updateTreeValidity(opts = { emitEvent: true }) {
		this._forEachChild((ctrl) => ctrl._updateTreeValidity(opts));
		this.updateValueAndValidity({
			onlySelf: true,
			emitEvent: opts.emitEvent
		});
	}
	_setInitialStatus() {
		this.status = this._allControlsDisabled() ? DISABLED : VALID;
	}
	_runValidator() {
		return this.validator ? this.validator(this) : null;
	}
	_runAsyncValidator(shouldHaveEmitted, emitEvent) {
		if (this.asyncValidator) {
			this.status = PENDING;
			this._hasOwnPendingAsyncValidator = {
				emitEvent: emitEvent !== false,
				shouldHaveEmitted: shouldHaveEmitted !== false
			};
			const obs = toObservable(this.asyncValidator(this));
			this._asyncValidationSubscription = obs.subscribe((errors) => {
				this._hasOwnPendingAsyncValidator = null;
				this.setErrors(errors, {
					emitEvent,
					shouldHaveEmitted
				});
			});
		}
	}
	_cancelExistingSubscription() {
		if (this._asyncValidationSubscription) {
			this._asyncValidationSubscription.unsubscribe();
			const shouldHaveEmitted = (this._hasOwnPendingAsyncValidator?.emitEvent || this._hasOwnPendingAsyncValidator?.shouldHaveEmitted) ?? false;
			this._hasOwnPendingAsyncValidator = null;
			return shouldHaveEmitted;
		}
		return false;
	}
	setErrors(errors, opts = {}) {
		this.errors = errors;
		this._updateControlsErrors(opts.emitEvent !== false, this, opts.shouldHaveEmitted);
	}
	get(path) {
		let currPath = path;
		if (currPath == null) return null;
		if (!Array.isArray(currPath)) currPath = currPath.split(".");
		if (currPath.length === 0) return null;
		return currPath.reduce((control, name) => control && control._find(name), this);
	}
	getError(errorCode, path) {
		const control = path ? this.get(path) : this;
		return control?.errors ? control.errors[errorCode] : null;
	}
	hasError(errorCode, path) {
		return !!this.getError(errorCode, path);
	}
	get root() {
		let x = this;
		while (x._parent) x = x._parent;
		return x;
	}
	_updateControlsErrors(emitEvent, changedControl, shouldHaveEmitted) {
		this.status = this._calculateStatus();
		if (emitEvent) this.statusChanges.emit(this.status);
		if (emitEvent || shouldHaveEmitted) this._events.next(new StatusChangeEvent(this.status, changedControl));
		if (this._parent) this._parent._updateControlsErrors(emitEvent, changedControl, shouldHaveEmitted);
	}
	_initObservables() {
		this.valueChanges = new EventEmitter();
		this.statusChanges = new EventEmitter();
	}
	_calculateStatus() {
		if (this._allControlsDisabled()) return DISABLED;
		if (this.errors) return INVALID;
		if (this._hasOwnPendingAsyncValidator || this._anyControlsHaveStatus(PENDING)) return PENDING;
		if (this._anyControlsHaveStatus(INVALID)) return INVALID;
		return VALID;
	}
	_anyControlsHaveStatus(status) {
		return this._anyControls((control) => control.status === status);
	}
	_anyControlsDirty() {
		return this._anyControls((control) => control.dirty);
	}
	_anyControlsTouched() {
		return this._anyControls((control) => control.touched);
	}
	_updatePristine(opts, changedControl) {
		const newPristine = !this._anyControlsDirty();
		const changed = this.pristine !== newPristine;
		this.pristine = newPristine;
		if (!opts.onlySelf) this._parent?._updatePristine(opts, changedControl);
		if (changed) this._events.next(new PristineChangeEvent(this.pristine, changedControl));
	}
	_updateTouched(opts = {}, changedControl) {
		this.touched = this._anyControlsTouched();
		this._events.next(new TouchedChangeEvent(this.touched, changedControl));
		if (!opts.onlySelf) this._parent?._updateTouched(opts, changedControl);
	}
	_onDisabledChange = [];
	_registerOnCollectionChange(fn) {
		this._onCollectionChange = fn;
	}
	_setUpdateStrategy(opts) {
		if (isOptionsObj(opts) && opts.updateOn != null) this._updateOn = opts.updateOn;
	}
	_parentMarkedDirty(onlySelf) {
		return !onlySelf && !!this._parent?.dirty && !this._parent._anyControlsDirty();
	}
	_find(name) {
		return null;
	}
	_assignValidators(validators) {
		this._rawValidators = Array.isArray(validators) ? validators.slice() : validators;
		this._composedValidatorFn = coerceToValidator(this._rawValidators);
		this._updateHasRequiredValidator();
	}
	_assignAsyncValidators(validators) {
		this._rawAsyncValidators = Array.isArray(validators) ? validators.slice() : validators;
		this._composedAsyncValidatorFn = coerceToAsyncValidator(this._rawAsyncValidators);
	}
	_updateHasRequiredValidator() {
		untracked(() => this._hasRequired.set(this.hasValidator(Validators.required)));
	}
};
function hasOwnControl(controls, name) {
	return Object.hasOwn(controls, name);
}
function isNativeFormElement(element) {
	return element.tagName === "INPUT" || element.tagName === "SELECT" || element.tagName === "TEXTAREA";
}
function elementAcceptsMinMax(element) {
	if (element.tagName !== "INPUT") return false;
	const type = element.type;
	return type === "number" || type === "range" || type === "date" || type === "month";
}
function isTextualFormElement(element) {
	return element.tagName === "INPUT" || element.tagName === "TEXTAREA";
}
function setNativeDomProperty(renderer, element, name, value) {
	switch (name) {
		case "name":
			renderer.setAttribute(element, name, value);
			break;
		case "disabled":
		case "readonly":
		case "required":
			if (value) renderer.setAttribute(element, name, "");
			else renderer.removeAttribute(element, name);
			break;
		case "max":
		case "min":
		case "minLength":
		case "maxLength": if (value !== void 0) renderer.setAttribute(element, name, value.toString());
		else renderer.removeAttribute(element, name);
	}
}
var ReactiveValidationError = class {
	kind;
	context;
	control;
	message;
	constructor({ kind, context, control }) {
		this.kind = kind;
		this.context = context;
		this.control = control;
	}
};
function toInteger(value) {
	return typeof value === "number" ? value : parseInt(value, 10);
}
function toFloat(value) {
	return typeof value === "number" ? value : parseFloat(value);
}
var AbstractValidatorDirective = class AbstractValidatorDirective {
	_validator = nullValidator;
	_onChange;
	_enabled;
	ngOnChanges(changes) {
		if (this.inputName in changes) {
			const input = this.normalizeInput(changes[this.inputName].currentValue);
			this._enabled = this.enabled(input);
			this._validator = this._enabled ? this.createValidator(input) : nullValidator;
			this._onChange?.();
		}
	}
	validate(control) {
		return this._validator(control);
	}
	registerOnValidatorChange(fn) {
		this._onChange = fn;
	}
	enabled(input) {
		return input != null;
	}
	static ɵfac = function AbstractValidatorDirective_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || AbstractValidatorDirective)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: AbstractValidatorDirective,
		features: [ɵɵNgOnChangesFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(AbstractValidatorDirective, [{ type: Directive }], null, null);
})();
var MAX_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => MaxValidator),
	multi: true
};
var MaxValidator = class MaxValidator extends AbstractValidatorDirective {
	max;
	inputName = "max";
	normalizeInput = (input) => toFloat(input);
	createValidator = (max) => maxValidator(max);
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMaxValidator_BaseFactory = void 0;
		return function MaxValidator_Factory(__ngFactoryType__) {
			return (ɵMaxValidator_BaseFactory || (ɵMaxValidator_BaseFactory = ɵɵgetInheritedFactory(MaxValidator)))(__ngFactoryType__ || MaxValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MaxValidator,
		selectors: [
			[
				"input",
				"type",
				"number",
				"max",
				"",
				"formControlName",
				""
			],
			[
				"input",
				"type",
				"number",
				"max",
				"",
				"formControl",
				""
			],
			[
				"input",
				"type",
				"number",
				"max",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function MaxValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("max", ctx._enabled ? ctx.max : null);
		},
		inputs: { max: "max" },
		standalone: false,
		features: [ɵɵProvidersFeature([MAX_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MaxValidator, [{
		type: Directive,
		args: [{
			selector: "input[type=number][max][formControlName],input[type=number][max][formControl],input[type=number][max][ngModel]",
			providers: [MAX_VALIDATOR],
			host: { "[attr.max]": "_enabled ? max : null" },
			standalone: false
		}]
	}], null, { max: [{ type: Input }] });
})();
var MIN_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => MinValidator),
	multi: true
};
var MinValidator = class MinValidator extends AbstractValidatorDirective {
	min;
	inputName = "min";
	normalizeInput = (input) => toFloat(input);
	createValidator = (min) => minValidator(min);
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMinValidator_BaseFactory = void 0;
		return function MinValidator_Factory(__ngFactoryType__) {
			return (ɵMinValidator_BaseFactory || (ɵMinValidator_BaseFactory = ɵɵgetInheritedFactory(MinValidator)))(__ngFactoryType__ || MinValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MinValidator,
		selectors: [
			[
				"input",
				"type",
				"number",
				"min",
				"",
				"formControlName",
				""
			],
			[
				"input",
				"type",
				"number",
				"min",
				"",
				"formControl",
				""
			],
			[
				"input",
				"type",
				"number",
				"min",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function MinValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("min", ctx._enabled ? ctx.min : null);
		},
		inputs: { min: "min" },
		standalone: false,
		features: [ɵɵProvidersFeature([MIN_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MinValidator, [{
		type: Directive,
		args: [{
			selector: "input[type=number][min][formControlName],input[type=number][min][formControl],input[type=number][min][ngModel]",
			providers: [MIN_VALIDATOR],
			host: { "[attr.min]": "_enabled ? min : null" },
			standalone: false
		}]
	}], null, { min: [{ type: Input }] });
})();
var REQUIRED_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => RequiredValidator),
	multi: true
};
var CHECKBOX_REQUIRED_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => CheckboxRequiredValidator),
	multi: true
};
var RequiredValidator = class RequiredValidator extends AbstractValidatorDirective {
	required;
	inputName = "required";
	normalizeInput = booleanAttribute;
	createValidator = (input) => requiredValidator;
	enabled(input) {
		return input;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵRequiredValidator_BaseFactory = void 0;
		return function RequiredValidator_Factory(__ngFactoryType__) {
			return (ɵRequiredValidator_BaseFactory || (ɵRequiredValidator_BaseFactory = ɵɵgetInheritedFactory(RequiredValidator)))(__ngFactoryType__ || RequiredValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: RequiredValidator,
		selectors: [
			[
				"",
				"required",
				"",
				"formControlName",
				"",
				3,
				"type",
				"checkbox"
			],
			[
				"",
				"required",
				"",
				"formControl",
				"",
				3,
				"type",
				"checkbox"
			],
			[
				"",
				"required",
				"",
				"ngModel",
				"",
				3,
				"type",
				"checkbox"
			]
		],
		hostVars: 1,
		hostBindings: function RequiredValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("required", ctx._enabled ? "" : null);
		},
		inputs: { required: "required" },
		standalone: false,
		features: [ɵɵProvidersFeature([REQUIRED_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(RequiredValidator, [{
		type: Directive,
		args: [{
			selector: ":not([type=checkbox])[required][formControlName],:not([type=checkbox])[required][formControl],:not([type=checkbox])[required][ngModel]",
			providers: [REQUIRED_VALIDATOR],
			host: { "[attr.required]": "_enabled ? \"\" : null" },
			standalone: false
		}]
	}], null, { required: [{ type: Input }] });
})();
var CheckboxRequiredValidator = class CheckboxRequiredValidator extends RequiredValidator {
	createValidator = (input) => requiredTrueValidator;
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵCheckboxRequiredValidator_BaseFactory = void 0;
		return function CheckboxRequiredValidator_Factory(__ngFactoryType__) {
			return (ɵCheckboxRequiredValidator_BaseFactory || (ɵCheckboxRequiredValidator_BaseFactory = ɵɵgetInheritedFactory(CheckboxRequiredValidator)))(__ngFactoryType__ || CheckboxRequiredValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: CheckboxRequiredValidator,
		selectors: [
			[
				"input",
				"type",
				"checkbox",
				"required",
				"",
				"formControlName",
				""
			],
			[
				"input",
				"type",
				"checkbox",
				"required",
				"",
				"formControl",
				""
			],
			[
				"input",
				"type",
				"checkbox",
				"required",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function CheckboxRequiredValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("required", ctx._enabled ? "" : null);
		},
		standalone: false,
		features: [ɵɵProvidersFeature([CHECKBOX_REQUIRED_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(CheckboxRequiredValidator, [{
		type: Directive,
		args: [{
			selector: "input[type=checkbox][required][formControlName],input[type=checkbox][required][formControl],input[type=checkbox][required][ngModel]",
			providers: [CHECKBOX_REQUIRED_VALIDATOR],
			host: { "[attr.required]": "_enabled ? \"\" : null" },
			standalone: false
		}]
	}], null, null);
})();
var EMAIL_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => EmailValidator),
	multi: true
};
var EmailValidator = class EmailValidator extends AbstractValidatorDirective {
	email;
	inputName = "email";
	normalizeInput = booleanAttribute;
	createValidator = (input) => emailValidator;
	enabled(input) {
		return input;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵEmailValidator_BaseFactory = void 0;
		return function EmailValidator_Factory(__ngFactoryType__) {
			return (ɵEmailValidator_BaseFactory || (ɵEmailValidator_BaseFactory = ɵɵgetInheritedFactory(EmailValidator)))(__ngFactoryType__ || EmailValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: EmailValidator,
		selectors: [
			[
				"",
				"email",
				"",
				"formControlName",
				""
			],
			[
				"",
				"email",
				"",
				"formControl",
				""
			],
			[
				"",
				"email",
				"",
				"ngModel",
				""
			]
		],
		inputs: { email: "email" },
		standalone: false,
		features: [ɵɵProvidersFeature([EMAIL_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(EmailValidator, [{
		type: Directive,
		args: [{
			selector: "[email][formControlName],[email][formControl],[email][ngModel]",
			providers: [EMAIL_VALIDATOR],
			standalone: false
		}]
	}], null, { email: [{ type: Input }] });
})();
var MIN_LENGTH_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => MinLengthValidator),
	multi: true
};
var MinLengthValidator = class MinLengthValidator extends AbstractValidatorDirective {
	minlength;
	inputName = "minlength";
	normalizeInput = (input) => toInteger(input);
	createValidator = (minlength) => minLengthValidator(minlength);
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMinLengthValidator_BaseFactory = void 0;
		return function MinLengthValidator_Factory(__ngFactoryType__) {
			return (ɵMinLengthValidator_BaseFactory || (ɵMinLengthValidator_BaseFactory = ɵɵgetInheritedFactory(MinLengthValidator)))(__ngFactoryType__ || MinLengthValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MinLengthValidator,
		selectors: [
			[
				"",
				"minlength",
				"",
				"formControlName",
				""
			],
			[
				"",
				"minlength",
				"",
				"formControl",
				""
			],
			[
				"",
				"minlength",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function MinLengthValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("minlength", ctx._enabled ? ctx.minlength : null);
		},
		inputs: { minlength: "minlength" },
		standalone: false,
		features: [ɵɵProvidersFeature([MIN_LENGTH_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MinLengthValidator, [{
		type: Directive,
		args: [{
			selector: "[minlength][formControlName],[minlength][formControl],[minlength][ngModel]",
			providers: [MIN_LENGTH_VALIDATOR],
			host: { "[attr.minlength]": "_enabled ? minlength : null" },
			standalone: false
		}]
	}], null, { minlength: [{ type: Input }] });
})();
var MAX_LENGTH_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => MaxLengthValidator),
	multi: true
};
var MaxLengthValidator = class MaxLengthValidator extends AbstractValidatorDirective {
	maxlength;
	inputName = "maxlength";
	normalizeInput = (input) => toInteger(input);
	createValidator = (maxlength) => maxLengthValidator(maxlength);
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMaxLengthValidator_BaseFactory = void 0;
		return function MaxLengthValidator_Factory(__ngFactoryType__) {
			return (ɵMaxLengthValidator_BaseFactory || (ɵMaxLengthValidator_BaseFactory = ɵɵgetInheritedFactory(MaxLengthValidator)))(__ngFactoryType__ || MaxLengthValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MaxLengthValidator,
		selectors: [
			[
				"",
				"maxlength",
				"",
				"formControlName",
				""
			],
			[
				"",
				"maxlength",
				"",
				"formControl",
				""
			],
			[
				"",
				"maxlength",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function MaxLengthValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("maxlength", ctx._enabled ? ctx.maxlength : null);
		},
		inputs: { maxlength: "maxlength" },
		standalone: false,
		features: [ɵɵProvidersFeature([MAX_LENGTH_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MaxLengthValidator, [{
		type: Directive,
		args: [{
			selector: "[maxlength][formControlName],[maxlength][formControl],[maxlength][ngModel]",
			providers: [MAX_LENGTH_VALIDATOR],
			host: { "[attr.maxlength]": "_enabled ? maxlength : null" },
			standalone: false
		}]
	}], null, { maxlength: [{ type: Input }] });
})();
var PATTERN_VALIDATOR = {
	provide: NG_VALIDATORS,
	useExisting: forwardRef(() => PatternValidator),
	multi: true
};
var PatternValidator = class PatternValidator extends AbstractValidatorDirective {
	pattern;
	inputName = "pattern";
	normalizeInput = (input) => input;
	createValidator = (input) => patternValidator(input);
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵPatternValidator_BaseFactory = void 0;
		return function PatternValidator_Factory(__ngFactoryType__) {
			return (ɵPatternValidator_BaseFactory || (ɵPatternValidator_BaseFactory = ɵɵgetInheritedFactory(PatternValidator)))(__ngFactoryType__ || PatternValidator);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: PatternValidator,
		selectors: [
			[
				"",
				"pattern",
				"",
				"formControlName",
				""
			],
			[
				"",
				"pattern",
				"",
				"formControl",
				""
			],
			[
				"",
				"pattern",
				"",
				"ngModel",
				""
			]
		],
		hostVars: 1,
		hostBindings: function PatternValidator_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("pattern", ctx._enabled ? ctx.pattern : null);
		},
		inputs: { pattern: "pattern" },
		standalone: false,
		features: [ɵɵProvidersFeature([PATTERN_VALIDATOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(PatternValidator, [{
		type: Directive,
		args: [{
			selector: "[pattern][formControlName],[pattern][formControl],[pattern][ngModel]",
			providers: [PATTERN_VALIDATOR],
			host: { "[attr.pattern]": "_enabled ? pattern : null" },
			standalone: false
		}]
	}], null, { pattern: [{ type: Input }] });
})();
var ɵFORM_CONTROL_INTEGRATION = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "FORM_CONTROL_INTEGRATION" : "");
var CALL_SET_DISABLED_STATE = new InjectionToken(typeof ngDevMode === "undefined" || ngDevMode ? "CallSetDisabledState" : "", { factory: () => setDisabledStateDefault });
var setDisabledStateDefault = "always";
function controlPath(name, parent) {
	return [...parent.path, name];
}
function setUpControlValueAccessor(control, dir, callSetDisabledState = setDisabledStateDefault) {
	if (typeof ngDevMode === "undefined" || ngDevMode) {
		if (!control) _throwError(dir, "Cannot find control with");
		if (!dir.valueAccessor) _throwMissingValueAccessorError(dir);
	}
	setUpValidators(control, dir);
	dir.valueAccessor.writeValue(control.value);
	if (control.disabled || callSetDisabledState === "always") dir.valueAccessor.setDisabledState?.(control.disabled);
	setUpViewChangePipeline(control, dir);
	setUpModelChangePipeline(control, dir);
	setUpBlurPipeline(control, dir);
	setUpDisabledChangeHandler(control, dir);
}
function cleanUpControl(control, dir, validateControlPresenceOnChange = true) {
	const noop = () => {
		if (validateControlPresenceOnChange && (typeof ngDevMode === "undefined" || ngDevMode)) _noControlError(dir);
	};
	dir?.valueAccessor?.registerOnChange(noop);
	dir?.valueAccessor?.registerOnTouched(noop);
	cleanUpValidators(control, dir);
	if (control) {
		dir._invokeOnDestroyCallbacks();
		control._registerOnCollectionChange(() => {});
	}
}
function registerOnValidatorChange(validators, onChange) {
	validators.forEach((validator) => {
		if (validator.registerOnValidatorChange) validator.registerOnValidatorChange(onChange);
	});
}
function setUpDisabledChangeHandler(control, dir) {
	if (dir.valueAccessor.setDisabledState) {
		const onDisabledChange = (isDisabled) => {
			dir.valueAccessor.setDisabledState(isDisabled);
		};
		control.registerOnDisabledChange(onDisabledChange);
		dir._registerOnDestroy(() => {
			control._unregisterOnDisabledChange(onDisabledChange);
		});
	}
}
function setUpValidators(control, dir) {
	const validators = getControlValidators(control);
	if (dir.validator !== null) control.setValidators(mergeValidators(validators, dir.validator));
	else if (typeof validators === "function") control.setValidators([validators]);
	const asyncValidators = getControlAsyncValidators(control);
	if (dir.asyncValidator !== null) control.setAsyncValidators(mergeValidators(asyncValidators, dir.asyncValidator));
	else if (typeof asyncValidators === "function") control.setAsyncValidators([asyncValidators]);
	const onValidatorChange = () => control.updateValueAndValidity();
	registerOnValidatorChange(dir._rawValidators, onValidatorChange);
	registerOnValidatorChange(dir._rawAsyncValidators, onValidatorChange);
}
function cleanUpValidators(control, dir) {
	let isControlUpdated = false;
	if (control !== null) {
		if (dir.validator !== null) {
			const validators = getControlValidators(control);
			if (Array.isArray(validators) && validators.length > 0) {
				const updatedValidators = validators.filter((validator) => validator !== dir.validator);
				if (updatedValidators.length !== validators.length) {
					isControlUpdated = true;
					control.setValidators(updatedValidators);
				}
			}
		}
		if (dir.asyncValidator !== null) {
			const asyncValidators = getControlAsyncValidators(control);
			if (Array.isArray(asyncValidators) && asyncValidators.length > 0) {
				const updatedAsyncValidators = asyncValidators.filter((asyncValidator) => asyncValidator !== dir.asyncValidator);
				if (updatedAsyncValidators.length !== asyncValidators.length) {
					isControlUpdated = true;
					control.setAsyncValidators(updatedAsyncValidators);
				}
			}
		}
	}
	const noop = () => {};
	registerOnValidatorChange(dir._rawValidators, noop);
	registerOnValidatorChange(dir._rawAsyncValidators, noop);
	return isControlUpdated;
}
function setUpViewChangePipeline(control, dir) {
	dir.valueAccessor.registerOnChange((newValue) => {
		control._pendingValue = newValue;
		control._pendingChange = true;
		control._pendingDirty = true;
		if (control.updateOn === "change") updateControl(control, dir);
	});
}
function setUpBlurPipeline(control, dir) {
	dir.valueAccessor.registerOnTouched(() => {
		control._pendingTouched = true;
		if (control.updateOn === "blur" && control._pendingChange) updateControl(control, dir);
		if (control.updateOn !== "submit") control.markAsTouched();
	});
}
function updateControl(control, dir) {
	if (control._pendingDirty) control.markAsDirty();
	control.setValue(control._pendingValue, { emitModelToViewChange: false });
	dir.viewToModelUpdate(control._pendingValue);
	control._pendingChange = false;
}
function setUpModelChangePipeline(control, dir) {
	const onChange = (newValue, emitModelEvent) => {
		dir.valueAccessor.writeValue(newValue);
		if (emitModelEvent) dir.viewToModelUpdate(newValue);
	};
	control.registerOnChange(onChange);
	dir._registerOnDestroy(() => {
		control._unregisterOnChange(onChange);
	});
}
function setUpFormContainer(control, dir) {
	if (control == null && (typeof ngDevMode === "undefined" || ngDevMode)) _throwError(dir, "Cannot find control with");
	setUpValidators(control, dir);
}
function cleanUpFormContainer(control, dir) {
	return cleanUpValidators(control, dir);
}
function _noControlError(dir) {
	return _throwError(dir, "There is no FormControl instance attached to form control element with");
}
function _throwError(dir, message) {
	const messageEnd = _describeControlLocation(dir);
	throw new Error(`${message} ${messageEnd}`);
}
function _describeControlLocation(dir) {
	const path = dir.path;
	if (path && path.length > 1) return `path: '${path.join(" -> ")}'`;
	if (path?.[0]) return `name: '${path}'`;
	return "unspecified name attribute";
}
function _throwMissingValueAccessorError(dir) {
	const loc = _describeControlLocation(dir);
	throw new RuntimeError(-1203, `No value accessor for form control ${loc}.`);
}
function _throwInvalidValueAccessorError(dir) {
	const loc = _describeControlLocation(dir);
	throw new RuntimeError(1200, `Value accessor was not provided as an array for form control with ${loc}. Check that the \`NG_VALUE_ACCESSOR\` token is configured as a \`multi: true\` provider.`);
}
function isPropertyUpdated(changes, viewModel) {
	if (!Object.hasOwn(changes, "model")) return false;
	const change = changes["model"];
	if (change.isFirstChange()) return true;
	return !Object.is(viewModel, change.currentValue);
}
function isBuiltInAccessor(valueAccessor) {
	return Object.getPrototypeOf(valueAccessor.constructor) === BuiltInControlValueAccessor;
}
function syncPendingControls(form, directives) {
	form._syncPendingControls();
	directives.forEach((dir) => {
		const control = dir.control;
		if (control.updateOn === "submit" && control._pendingChange) {
			dir.viewToModelUpdate(control._pendingValue);
			control._pendingChange = false;
		}
	});
}
function selectValueAccessor(dir, valueAccessors) {
	if (!valueAccessors) return null;
	if (!Array.isArray(valueAccessors) && (typeof ngDevMode === "undefined" || ngDevMode)) _throwInvalidValueAccessorError(dir);
	let defaultAccessor = void 0;
	let builtinAccessor = void 0;
	let customAccessor = void 0;
	valueAccessors.forEach((v) => {
		if (v.constructor === DefaultValueAccessor) defaultAccessor = v;
		else if (isBuiltInAccessor(v)) {
			if (builtinAccessor && (typeof ngDevMode === "undefined" || ngDevMode)) _throwError(dir, "More than one built-in value accessor matches form control with");
			builtinAccessor = v;
		} else {
			if (customAccessor && (typeof ngDevMode === "undefined" || ngDevMode)) _throwError(dir, "More than one custom value accessor matches form control with");
			customAccessor = v;
		}
	});
	if (customAccessor) return customAccessor;
	if (builtinAccessor) return builtinAccessor;
	if (defaultAccessor) return defaultAccessor;
	if (typeof ngDevMode === "undefined" || ngDevMode) _throwError(dir, "No valid value accessor for form control with");
	return null;
}
function removeListItem$1(list, el) {
	const index = list.indexOf(el);
	if (index > -1) list.splice(index, 1);
}
function _ngModelWarning(name, type, instance, warningConfig) {
	if (warningConfig === "never") return;
	if ((warningConfig === null || warningConfig === "once") && !type._ngModelWarningSentOnce || warningConfig === "always" && !instance._ngModelWarningSent) {
		console.warn(ngModelWarning(name));
		type._ngModelWarningSentOnce = true;
		instance._ngModelWarningSent = true;
	}
}
var NG_CONTROL_INTEGRATION_PROVIDER = {
	provide: ɵFORM_CONTROL_INTEGRATION,
	useFactory: () => {
		const control = inject(NgControl, { self: true });
		return {
			setParseErrors: (source) => {
				control.setParseErrorSource(source);
			},
			set onReset(callback) {
				control.onReset = callback;
			}
		};
	}
};
var NgControl = class extends AbstractControlDirective {
	_parent = null;
	name = null;
	valueAccessor = null;
	isCustomControlBased = false;
	userOnReset;
	resetSubscription;
	set onReset(callback) {
		this.userOnReset = callback;
		this.resetSubscription?.unsubscribe();
		this.resetSubscription = void 0;
		if (this.control) {
			this.resetSubscription = this.control.events.subscribe((event) => {
				if (event instanceof FormResetEvent && this.control) this.userOnReset?.(this.control.value);
			});
			this.subscription?.add(this.resetSubscription);
		}
	}
	isNativeFormElement = false;
	rawValueAccessors;
	_selectedValueAccessor = null;
	get selectedValueAccessor() {
		return this._selectedValueAccessor ??= selectValueAccessor(this, this.rawValueAccessors);
	}
	parseErrorsValidator = null;
	renderer;
	injector;
	requiredValidatorViaDi;
	subscription;
	customControlBindings = null;
	constructor(injector, renderer, rawValueAccessors) {
		super();
		this.injector = injector;
		this.renderer = renderer;
		this.rawValueAccessors = rawValueAccessors;
		this.injector?.get(DestroyRef)?.onDestroy(() => {
			this.removeParseErrorsValidator(this.control);
			this.subscription?.unsubscribe();
		});
	}
	setupCustomControl() {
		this.subscription?.unsubscribe();
		const cdr = this.injector?.get(ChangeDetectorRef);
		if (!this.control || !cdr) return;
		const markForCheck = cdr.markForCheck.bind(cdr);
		this.subscription = new Subscription();
		this.subscription.add(this.control.valueChanges.subscribe(markForCheck));
		this.subscription.add(this.control.statusChanges.subscribe(markForCheck));
		this.resetSubscription?.unsubscribe();
		this.resetSubscription = void 0;
		if (this.userOnReset) {
			this.resetSubscription = this.control.events.subscribe((event) => {
				if (event instanceof FormResetEvent && this.control) this.userOnReset?.(this.control.value);
			});
			this.subscription.add(this.resetSubscription);
		}
		if (this.parseErrorsValidator) this.control.addValidators(this.parseErrorsValidator);
	}
	ngControlCreate(host) {
		if (!host.nativeElement.hasAttribute?.("ngNoCva") && (this.rawValueAccessors && this.rawValueAccessors.length > 0 || this.valueAccessor !== null) || !host.customControl) return;
		this.isCustomControlBased = true;
		host.listenToCustomControlModel((value) => {
			this.control?.markAsDirty();
			this.control?.setValue(value, { emitModelToViewChange: false });
			this.viewToModelUpdate(value);
		});
		host.listenToCustomControlOutput("touch", () => {
			this.control?.markAsTouched();
		});
		this.customControlBindings = {};
		this.isNativeFormElement = isNativeFormElement(host.nativeElement);
		this.requiredValidatorViaDi = this._rawValidators.find((v) => v instanceof RequiredValidator);
	}
	ngControlUpdate(host, bindRequired) {
		if (!this.isCustomControlBased) return;
		const control = this.control;
		const bindings = this.customControlBindings;
		if (!Object.is(bindings.value, control.value)) {
			bindings.value = control.value;
			host.setCustomControlModelInput(control.value);
		}
		this.bindControlProperty(host, bindings, "touched", control.touched);
		this.bindControlProperty(host, bindings, "dirty", control.dirty);
		this.bindControlProperty(host, bindings, "valid", control.valid);
		this.bindControlProperty(host, bindings, "invalid", control.invalid);
		this.bindControlProperty(host, bindings, "pending", control.pending);
		this.bindControlProperty(host, bindings, "disabled", control.disabled);
		if (this.shouldBindRequired) this.bindControlProperty(host, bindings, "required", this.isRequired);
		const errorObject = control.errors;
		if (bindings.errors !== errorObject) {
			bindings.errors = errorObject;
			const errorArray = this._convertErrors(errorObject);
			host.setInputOnDirectives("errors", errorArray);
		}
	}
	get isRequired() {
		return (this.requiredValidatorViaDi?._enabled || this.control?._hasRequired()) ?? false;
	}
	get shouldBindRequired() {
		return true;
	}
	bindControlProperty(host, bindings, name, value) {
		if (bindings[name] === value) return;
		bindings[name] = value;
		const wasSet = host.setInputOnDirectives(name, value);
		if (this.isNativeFormElement && !wasSet && (name === "disabled" || name === "required") && this.renderer) setNativeDomProperty(this.renderer, host.nativeElement, name, value);
	}
	_convertErrors(errors) {
		if (errors === null) return [];
		const control = this.control;
		return Object.entries(errors).map(([kind, context]) => {
			return new ReactiveValidationError({
				context,
				kind,
				control
			});
		});
	}
	setParseErrorSource(parseErrors) {
		if (parseErrors === void 0) return;
		let convertedErrors = null;
		const convertedParseErrors = computed(() => {
			const rawErrors = parseErrors();
			if (rawErrors.length === 0) return null;
			return rawErrors.reduce((acc, err) => {
				acc[err.kind] = err;
				return acc;
			}, {});
		}, ...ngDevMode ? [{ debugName: "convertedParseErrors" }] : []);
		this.parseErrorsValidator = (() => convertedErrors).bind(this);
		effect(() => {
			convertedErrors = convertedParseErrors();
			this.control?.updateValueAndValidity({ emitEvent: false });
		}, { injector: this.injector });
	}
	removeParseErrorsValidator(control) {
		if (this.parseErrorsValidator) {
			control?.removeValidators(this.parseErrorsValidator);
			control?.updateValueAndValidity({ emitEvent: false });
		}
	}
};
var AbstractControlStatus = class {
	_cd;
	constructor(cd) {
		this._cd = cd;
	}
	get isTouched() {
		this._cd?.control?._touched?.();
		return !!this._cd?.control?.touched;
	}
	get isUntouched() {
		return !!this._cd?.control?.untouched;
	}
	get isPristine() {
		this._cd?.control?._pristine?.();
		return !!this._cd?.control?.pristine;
	}
	get isDirty() {
		return !!this._cd?.control?.dirty;
	}
	get isValid() {
		this._cd?.control?._status?.();
		return !!this._cd?.control?.valid;
	}
	get isInvalid() {
		return !!this._cd?.control?.invalid;
	}
	get isPending() {
		return !!this._cd?.control?.pending;
	}
	get isSubmitted() {
		this._cd?._submitted?.();
		return !!this._cd?.submitted;
	}
};
var ngControlStatusHost = {
	"[class.ng-untouched]": "isUntouched",
	"[class.ng-touched]": "isTouched",
	"[class.ng-pristine]": "isPristine",
	"[class.ng-dirty]": "isDirty",
	"[class.ng-valid]": "isValid",
	"[class.ng-invalid]": "isInvalid",
	"[class.ng-pending]": "isPending"
};
var NgControlStatus = class NgControlStatus extends AbstractControlStatus {
	constructor(cd) {
		super(cd);
	}
	static ɵfac = function NgControlStatus_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgControlStatus)(ɵɵdirectiveInject(NgControl, 2));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgControlStatus,
		selectors: [
			[
				"",
				"formControlName",
				""
			],
			[
				"",
				"ngModel",
				""
			],
			[
				"",
				"formControl",
				""
			]
		],
		hostVars: 14,
		hostBindings: function NgControlStatus_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵclassProp("ng-untouched", ctx.isUntouched)("ng-touched", ctx.isTouched)("ng-pristine", ctx.isPristine)("ng-dirty", ctx.isDirty)("ng-valid", ctx.isValid)("ng-invalid", ctx.isInvalid)("ng-pending", ctx.isPending);
		},
		standalone: false,
		features: [ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgControlStatus, [{
		type: Directive,
		args: [{
			selector: "[formControlName],[ngModel],[formControl]",
			host: ngControlStatusHost,
			standalone: false
		}]
	}], () => [{
		type: NgControl,
		decorators: [{ type: Self }]
	}], null);
})();
var NgControlStatusGroup = class NgControlStatusGroup extends AbstractControlStatus {
	constructor(cd) {
		super(cd);
	}
	static ɵfac = function NgControlStatusGroup_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgControlStatusGroup)(ɵɵdirectiveInject(ControlContainer, 10));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgControlStatusGroup,
		selectors: [
			[
				"",
				"formGroupName",
				""
			],
			[
				"",
				"formArrayName",
				""
			],
			[
				"",
				"ngModelGroup",
				""
			],
			[
				"",
				"formGroup",
				""
			],
			[
				"",
				"formArray",
				""
			],
			[
				"form",
				3,
				"ngNoForm",
				""
			],
			[
				"",
				"ngForm",
				""
			]
		],
		hostVars: 16,
		hostBindings: function NgControlStatusGroup_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵclassProp("ng-untouched", ctx.isUntouched)("ng-touched", ctx.isTouched)("ng-pristine", ctx.isPristine)("ng-dirty", ctx.isDirty)("ng-valid", ctx.isValid)("ng-invalid", ctx.isInvalid)("ng-pending", ctx.isPending)("ng-submitted", ctx.isSubmitted);
		},
		standalone: false,
		features: [ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgControlStatusGroup, [{
		type: Directive,
		args: [{
			selector: "[formGroupName],[formArrayName],[ngModelGroup],[formGroup],[formArray],form:not([ngNoForm]),[ngForm]",
			host: {
				...ngControlStatusHost,
				"[class.ng-submitted]": "isSubmitted"
			},
			standalone: false
		}]
	}], () => [{
		type: ControlContainer,
		decorators: [{ type: Optional }, { type: Self }]
	}], null);
})();
var FormGroup = class extends AbstractControl {
	constructor(controls, validatorOrOpts, asyncValidator) {
		super(pickValidators(validatorOrOpts), pickAsyncValidators(asyncValidator, validatorOrOpts));
		(typeof ngDevMode === "undefined" || ngDevMode) && validateFormGroupControls(controls);
		this.controls = controls;
		this._initObservables();
		this._setUpdateStrategy(validatorOrOpts);
		this._setUpControls();
		this.updateValueAndValidity({
			onlySelf: true,
			emitEvent: !!this.asyncValidator
		});
	}
	controls;
	registerControl(name, control) {
		const existingControl = this._find(name);
		if (existingControl) return existingControl;
		this.controls[name] = control;
		control.setParent(this);
		control._registerOnCollectionChange(this._onCollectionChange);
		return control;
	}
	addControl(name, control, options = {}) {
		this.registerControl(name, control);
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
		this._onCollectionChange();
	}
	removeControl(name, options = {}) {
		const existingControl = this._find(name);
		if (existingControl) existingControl._registerOnCollectionChange(() => {});
		delete this.controls[name];
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
		this._onCollectionChange();
	}
	setControl(name, control, options = {}) {
		const existingControl = this._find(name);
		if (existingControl) existingControl._registerOnCollectionChange(() => {});
		delete this.controls[name];
		if (control) this.registerControl(name, control);
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
		this._onCollectionChange();
	}
	contains(controlName) {
		return this._find(controlName)?.enabled === true;
	}
	setValue(value, options = {}) {
		untracked(() => {
			assertAllValuesPresent(this, true, value);
			Object.keys(value).forEach((name) => {
				assertControlPresent(this, true, name);
				this.controls[name].setValue(value[name], {
					onlySelf: true,
					emitEvent: options.emitEvent
				});
			});
			this.updateValueAndValidity(options);
		});
	}
	patchValue(value, options = {}) {
		if (value == null) return;
		Object.keys(value).forEach((name) => {
			const existingControl = this._find(name);
			if (existingControl) existingControl.patchValue(value[name], {
				onlySelf: true,
				emitEvent: options.emitEvent
			});
		});
		this.updateValueAndValidity(options);
	}
	reset(value = {}, options = {}) {
		this._forEachChild((control, name) => {
			control.reset(value ? value[name] : null, {
				...options,
				onlySelf: true
			});
		});
		this._updatePristine(options, this);
		this._updateTouched(options, this);
		this.updateValueAndValidity(options);
		if (options?.emitEvent !== false) this._events.next(new FormResetEvent(this));
	}
	getRawValue() {
		return this._reduceChildren({}, (acc, control, name) => {
			acc[name] = control.getRawValue();
			return acc;
		});
	}
	_syncPendingControls() {
		let subtreeUpdated = this._reduceChildren(false, (updated, child) => {
			return child._syncPendingControls() ? true : updated;
		});
		if (subtreeUpdated) this.updateValueAndValidity({ onlySelf: true });
		return subtreeUpdated;
	}
	_forEachChild(cb) {
		Object.keys(this.controls).forEach((key) => {
			const control = this.controls[key];
			control && cb(control, key);
		});
	}
	_setUpControls() {
		this._forEachChild((control) => {
			control.setParent(this);
			control._registerOnCollectionChange(this._onCollectionChange);
		});
	}
	_updateValue() {
		this.value = this._reduceValue();
	}
	_anyControls(condition) {
		for (const [controlName, control] of Object.entries(this.controls)) if (this.contains(controlName) && condition(control)) return true;
		return false;
	}
	_reduceValue() {
		return this._reduceChildren({}, (acc, control, name) => {
			if (control.enabled || this.disabled) acc[name] = control.value;
			return acc;
		});
	}
	_reduceChildren(initValue, fn) {
		let res = initValue;
		this._forEachChild((control, name) => {
			res = fn(res, control, name);
		});
		return res;
	}
	_allControlsDisabled() {
		for (const controlName of Object.keys(this.controls)) if (this.controls[controlName].enabled) return false;
		return Object.keys(this.controls).length > 0 || this.disabled;
	}
	_find(name) {
		return hasOwnControl(this.controls, name) ? this.controls[name] : null;
	}
};
function validateFormGroupControls(controls) {
	const invalidKeys = Object.keys(controls).filter((key) => key.includes("."));
	if (invalidKeys.length > 0) console.warn(`FormGroup keys cannot include \`.\`, please replace the keys for: ${invalidKeys.join(",")}.`);
}
var FormRecord = class extends FormGroup {};
var formDirectiveProvider$2 = {
	provide: ControlContainer,
	useExisting: forwardRef(() => NgForm)
};
var resolvedPromise$1 = (() => Promise.resolve())();
var NgForm = class NgForm extends ControlContainer {
	callSetDisabledState;
	get submitted() {
		return untracked(this.submittedReactive);
	}
	_submitted = computed(() => this.submittedReactive(), ...ngDevMode ? [{ debugName: "_submitted" }] : []);
	submittedReactive = signal(false, ...ngDevMode ? [{ debugName: "submittedReactive" }] : []);
	_directives = /* @__PURE__ */ new Set();
	form;
	ngSubmit = new EventEmitter();
	options;
	constructor(validators, asyncValidators, callSetDisabledState) {
		super();
		this.callSetDisabledState = callSetDisabledState;
		this.form = new FormGroup({}, composeValidators(validators), composeAsyncValidators(asyncValidators));
	}
	ngAfterViewInit() {
		this._setUpdateStrategy();
	}
	get formDirective() {
		return this;
	}
	get control() {
		return this.form;
	}
	get path() {
		return [];
	}
	get controls() {
		return this.form.controls;
	}
	addControl(dir) {
		resolvedPromise$1.then(() => {
			dir.control = this._findContainer(dir.path).registerControl(dir.name, dir.control);
			dir._setupWithForm(this.callSetDisabledState);
			dir.control.updateValueAndValidity({ emitEvent: false });
			this._directives.add(dir);
		});
	}
	getControl(dir) {
		return this.form.get(dir.path);
	}
	removeControl(dir) {
		resolvedPromise$1.then(() => {
			this._findContainer(dir.path)?.removeControl(dir.name);
			this._directives.delete(dir);
		});
	}
	addFormGroup(dir) {
		resolvedPromise$1.then(() => {
			const container = this._findContainer(dir.path);
			const group = new FormGroup({});
			setUpFormContainer(group, dir);
			container.registerControl(dir.name, group);
			group.updateValueAndValidity({ emitEvent: false });
		});
	}
	removeFormGroup(dir) {
		resolvedPromise$1.then(() => {
			this._findContainer(dir.path)?.removeControl?.(dir.name);
		});
	}
	getFormGroup(dir) {
		return this.form.get(dir.path);
	}
	updateModel(dir, value) {
		resolvedPromise$1.then(() => {
			this.form.get(dir.path).setValue(value);
		});
	}
	setValue(value) {
		this.control.setValue(value);
	}
	onSubmit($event) {
		this.submittedReactive.set(true);
		syncPendingControls(this.form, this._directives);
		this.ngSubmit.emit($event);
		this.form._events.next(new FormSubmittedEvent(this.control));
		return $event?.target?.method === "dialog";
	}
	onReset() {
		this.resetForm();
	}
	resetForm(value = void 0) {
		this.form.reset(value);
		this.submittedReactive.set(false);
	}
	_setUpdateStrategy() {
		if (this.options && this.options.updateOn != null) this.form._updateOn = this.options.updateOn;
	}
	_findContainer(path) {
		path.pop();
		return path.length ? this.form.get(path) : this.form;
	}
	static ɵfac = function NgForm_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgForm)(ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10), ɵɵdirectiveInject(CALL_SET_DISABLED_STATE, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgForm,
		selectors: [
			[
				"form",
				3,
				"ngNoForm",
				"",
				3,
				"formGroup",
				"",
				3,
				"formArray",
				""
			],
			["ng-form"],
			[
				"",
				"ngForm",
				""
			]
		],
		hostBindings: function NgForm_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("submit", function NgForm_submit_HostBindingHandler($event) {
				return ctx.onSubmit($event);
			})("reset", function NgForm_reset_HostBindingHandler() {
				return ctx.onReset();
			});
		},
		inputs: { options: [
			0,
			"ngFormOptions",
			"options"
		] },
		outputs: { ngSubmit: "ngSubmit" },
		exportAs: ["ngForm"],
		standalone: false,
		features: [ɵɵProvidersFeature([formDirectiveProvider$2]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgForm, [{
		type: Directive,
		args: [{
			selector: "form:not([ngNoForm]):not([formGroup]):not([formArray]),ng-form,[ngForm]",
			providers: [formDirectiveProvider$2],
			host: {
				"(submit)": "onSubmit($event)",
				"(reset)": "onReset()"
			},
			outputs: ["ngSubmit"],
			exportAs: "ngForm",
			standalone: false
		}]
	}], () => [
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [CALL_SET_DISABLED_STATE]
			}]
		}
	], { options: [{
		type: Input,
		args: ["ngFormOptions"]
	}] });
})();
function removeListItem(list, el) {
	const index = list.indexOf(el);
	if (index > -1) list.splice(index, 1);
}
function isFormControlState(formState) {
	return typeof formState === "object" && formState !== null && Object.keys(formState).length === 2 && "value" in formState && "disabled" in formState;
}
var FormControl = class FormControl extends AbstractControl {
	defaultValue = null;
	_onChange = [];
	_pendingValue;
	_pendingChange = false;
	constructor(formState = null, validatorOrOpts, asyncValidator) {
		super(pickValidators(validatorOrOpts), pickAsyncValidators(asyncValidator, validatorOrOpts));
		this._applyFormState(formState);
		this._setUpdateStrategy(validatorOrOpts);
		this._initObservables();
		this.updateValueAndValidity({
			onlySelf: true,
			emitEvent: !!this.asyncValidator
		});
		if (isOptionsObj(validatorOrOpts) && (validatorOrOpts.nonNullable || validatorOrOpts.initialValueIsDefault)) {
			if (isFormControlState(formState)) this.defaultValue = formState.value;
			else this.defaultValue = formState;
		}
	}
	setValue(value, options = {}) {
		untracked(() => {
			this.value = this._pendingValue = value;
			if (this._onChange.length && options.emitModelToViewChange !== false) this._onChange.forEach((changeFn) => changeFn(this.value, options.emitViewToModelChange !== false));
			this.updateValueAndValidity(options);
		});
	}
	patchValue(value, options = {}) {
		this.setValue(value, options);
	}
	reset(formState = this.defaultValue, options = {}) {
		this._applyFormState(formState);
		this.markAsPristine(options);
		this.markAsUntouched(options);
		this.setValue(this.value, options);
		if (options.overwriteDefaultValue) this.defaultValue = this.value;
		this._pendingChange = false;
		if (options?.emitEvent !== false) this._events.next(new FormResetEvent(this));
	}
	_updateValue() {}
	_anyControls(condition) {
		return false;
	}
	_allControlsDisabled() {
		return this.disabled;
	}
	registerOnChange(fn) {
		this._onChange.push(fn);
	}
	_unregisterOnChange(fn) {
		removeListItem(this._onChange, fn);
	}
	registerOnDisabledChange(fn) {
		this._onDisabledChange.push(fn);
	}
	_unregisterOnDisabledChange(fn) {
		removeListItem(this._onDisabledChange, fn);
	}
	_forEachChild(cb) {}
	_syncPendingControls() {
		if (this.updateOn === "submit") {
			if (this._pendingDirty) this.markAsDirty();
			if (this._pendingTouched) this.markAsTouched();
			if (this._pendingChange) {
				this.setValue(this._pendingValue, {
					onlySelf: true,
					emitModelToViewChange: false
				});
				return true;
			}
		}
		return false;
	}
	_applyFormState(formState) {
		if (isFormControlState(formState)) {
			this.value = this._pendingValue = formState.value;
			formState.disabled ? this.disable({
				onlySelf: true,
				emitEvent: false
			}) : this.enable({
				onlySelf: true,
				emitEvent: false
			});
		} else this.value = this._pendingValue = formState;
	}
};
var isFormControl = (control) => control instanceof FormControl;
var AbstractFormGroupDirective = class AbstractFormGroupDirective extends ControlContainer {
	_parent;
	ngOnInit() {
		this._checkParentType();
		this.formDirective.addFormGroup(this);
	}
	ngOnDestroy() {
		this.formDirective?.removeFormGroup(this);
	}
	get control() {
		return this.formDirective.getFormGroup(this);
	}
	get path() {
		return controlPath(this.name == null ? this.name : this.name.toString(), this._parent);
	}
	get formDirective() {
		return this._parent ? this._parent.formDirective : null;
	}
	_checkParentType() {}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵAbstractFormGroupDirective_BaseFactory = void 0;
		return function AbstractFormGroupDirective_Factory(__ngFactoryType__) {
			return (ɵAbstractFormGroupDirective_BaseFactory || (ɵAbstractFormGroupDirective_BaseFactory = ɵɵgetInheritedFactory(AbstractFormGroupDirective)))(__ngFactoryType__ || AbstractFormGroupDirective);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: AbstractFormGroupDirective,
		standalone: false,
		features: [ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(AbstractFormGroupDirective, [{
		type: Directive,
		args: [{ standalone: false }]
	}], null, null);
})();
function modelParentException() {
	return new RuntimeError(1350, `
    ngModel cannot be used to register form controls with a parent formGroup directive.  Try using
    formGroup's partner directive "formControlName" instead.  Example:

    ${formControlNameExample}

    Or, if you'd like to avoid registering this form control, indicate that it's standalone in ngModelOptions:

    Example:

    ${ngModelWithFormGroupExample}`);
}
function formGroupNameException() {
	return new RuntimeError(1351, `
    ngModel cannot be used to register form controls with a parent formGroupName or formArrayName directive.

    Option 1: Use formControlName instead of ngModel (reactive strategy):

    ${formGroupNameExample}

    Option 2:  Update ngModel's parent be ngModelGroup (template-driven strategy):

    ${ngModelGroupExample}`);
}
function ngModelInChildComponentWarning(containerTypeName) {
	return formatRuntimeError(-1354, `ngModel on a form control inside a child component cannot register with the ${containerTypeName} in the parent component because @Host() stops injection at the component boundary. To register this control with the parent form, add viewProviders to the child component: @Component({ ..., viewProviders: [{ provide: ControlContainer, useExisting: ${containerTypeName} }] }). Or, to opt out of form registration, use [ngModelOptions]="{standalone: true}".`);
}
function missingNameException() {
	return new RuntimeError(1352, `If ngModel is used within a form tag, either the name attribute must be set or the form
    control must be defined as 'standalone' in ngModelOptions.

    Example 1: <input [(ngModel)]="person.firstName" name="first">
    Example 2: <input [(ngModel)]="person.firstName" [ngModelOptions]="{standalone: true}">`);
}
function modelGroupParentException() {
	return new RuntimeError(1353, `
    ngModelGroup cannot be used with a parent formGroup directive.

    Option 1: Use formGroupName instead of ngModelGroup (reactive strategy):

    ${formGroupNameExample}

    Option 2:  Use a regular form tag instead of the formGroup directive (template-driven strategy):

    ${ngModelGroupExample}`);
}
var modelGroupProvider = {
	provide: ControlContainer,
	useExisting: forwardRef(() => NgModelGroup)
};
var NgModelGroup = class NgModelGroup extends AbstractFormGroupDirective {
	name = "";
	constructor(parent, validators, asyncValidators) {
		super();
		this._parent = parent;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	_checkParentType() {
		if (!(this._parent instanceof NgModelGroup) && !(this._parent instanceof NgForm) && (typeof ngDevMode === "undefined" || ngDevMode)) throw modelGroupParentException();
	}
	static ɵfac = function NgModelGroup_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgModelGroup)(ɵɵdirectiveInject(ControlContainer, 5), ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgModelGroup,
		selectors: [[
			"",
			"ngModelGroup",
			""
		]],
		inputs: { name: [
			0,
			"ngModelGroup",
			"name"
		] },
		exportAs: ["ngModelGroup"],
		standalone: false,
		features: [ɵɵProvidersFeature([modelGroupProvider]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgModelGroup, [{
		type: Directive,
		args: [{
			selector: "[ngModelGroup]",
			providers: [modelGroupProvider],
			exportAs: "ngModelGroup",
			standalone: false
		}]
	}], () => [
		{
			type: ControlContainer,
			decorators: [{ type: Host }, { type: SkipSelf }]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		}
	], { name: [{
		type: Input,
		args: ["ngModelGroup"]
	}] });
})();
var AbstractFormDirective = class AbstractFormDirective extends ControlContainer {
	callSetDisabledState;
	get submitted() {
		return untracked(this._submittedReactive);
	}
	set submitted(value) {
		this._submittedReactive.set(value);
	}
	_submitted = computed(() => this._submittedReactive(), ...ngDevMode ? [{ debugName: "_submitted" }] : []);
	_submittedReactive = signal(false, ...ngDevMode ? [{ debugName: "_submittedReactive" }] : []);
	_oldForm;
	_onCollectionChange = () => this._updateDomValue();
	directives = [];
	constructor(validators, asyncValidators, callSetDisabledState) {
		super();
		this.callSetDisabledState = callSetDisabledState;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	ngOnChanges(changes) {
		this.onChanges(changes);
	}
	ngOnDestroy() {
		this.onDestroy();
	}
	onChanges(changes) {
		if (!this.form) throw new RuntimeError(1052, ngDevMode && `formGroup expects a FormGroup instance. Please pass one in.

      Example:

      ${formControlNameExample}`);
		if (Object.hasOwn(changes, "form")) {
			this._updateValidators();
			this._updateDomValue();
			this._updateRegistrations();
			this._oldForm = this.form;
		}
	}
	onDestroy() {
		if (this.form) {
			cleanUpValidators(this.form, this);
			if (this.form._onCollectionChange === this._onCollectionChange) this.form._registerOnCollectionChange(() => {});
		}
	}
	get formDirective() {
		return this;
	}
	get path() {
		return [];
	}
	addControl(dir) {
		const ctrl = this.form.get(dir.path);
		if (!ctrl) throw new RuntimeError(-1055, ngDevMode && missingControlForPathError(this.form, dir.path));
		dir._setupWithForm(ctrl, this.callSetDisabledState);
		ctrl.updateValueAndValidity({ emitEvent: false });
		this.directives.push(dir);
		return ctrl;
	}
	getControl(dir) {
		return this.form.get(dir.path);
	}
	removeControl(dir) {
		cleanUpControl(dir.control || null, dir, false);
		removeListItem$1(this.directives, dir);
	}
	addFormGroup(dir) {
		this._setUpFormContainer(dir);
	}
	removeFormGroup(dir) {
		this._cleanUpFormContainer(dir);
	}
	getFormGroup(dir) {
		return this.form.get(dir.path);
	}
	getFormArray(dir) {
		return this.form.get(dir.path);
	}
	addFormArray(dir) {
		this._setUpFormContainer(dir);
	}
	removeFormArray(dir) {
		this._cleanUpFormContainer(dir);
	}
	updateModel(dir, value) {
		this.form.get(dir.path).setValue(value);
	}
	onReset() {
		this.resetForm();
	}
	resetForm(value = void 0, options = {}) {
		this.form.reset(value, options);
		this._submittedReactive.set(false);
	}
	onSubmit($event) {
		this.submitted = true;
		syncPendingControls(this.form, this.directives);
		this.ngSubmit.emit($event);
		this.form._events.next(new FormSubmittedEvent(this.control));
		return $event?.target?.method === "dialog";
	}
	_updateDomValue() {
		this.directives.forEach((dir) => {
			const oldCtrl = dir.control;
			const newCtrl = this.form.get(dir.path);
			if (oldCtrl !== newCtrl) {
				cleanUpControl(oldCtrl || null, dir);
				if (isFormControl(newCtrl)) dir._setupWithForm(newCtrl, this.callSetDisabledState);
			}
		});
		this.form._updateTreeValidity({ emitEvent: false });
	}
	_setUpFormContainer(dir) {
		const ctrl = this.form.get(dir.path);
		setUpFormContainer(ctrl, dir);
		ctrl.updateValueAndValidity({ emitEvent: false });
	}
	_cleanUpFormContainer(dir) {
		const ctrl = this.form?.get(dir.path);
		if (ctrl) {
			if (cleanUpFormContainer(ctrl, dir)) ctrl.updateValueAndValidity({ emitEvent: false });
		}
	}
	_updateRegistrations() {
		this.form._registerOnCollectionChange(this._onCollectionChange);
		this._oldForm?._registerOnCollectionChange(() => {});
	}
	_updateValidators() {
		setUpValidators(this.form, this);
		if (this._oldForm) cleanUpValidators(this._oldForm, this);
	}
	static ɵfac = function AbstractFormDirective_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || AbstractFormDirective)(ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10), ɵɵdirectiveInject(CALL_SET_DISABLED_STATE, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: AbstractFormDirective,
		features: [ɵɵInheritDefinitionFeature, ɵɵNgOnChangesFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(AbstractFormDirective, [{ type: Directive }], () => [
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [CALL_SET_DISABLED_STATE]
			}]
		}
	], null);
})();
var formDirectiveProvider$1 = {
	provide: ControlContainer,
	useExisting: forwardRef(() => FormGroupDirective)
};
var FormGroupDirective = class FormGroupDirective extends AbstractFormDirective {
	form = null;
	ngSubmit = new EventEmitter();
	get control() {
		return this.form;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵFormGroupDirective_BaseFactory = void 0;
		return function FormGroupDirective_Factory(__ngFactoryType__) {
			return (ɵFormGroupDirective_BaseFactory || (ɵFormGroupDirective_BaseFactory = ɵɵgetInheritedFactory(FormGroupDirective)))(__ngFactoryType__ || FormGroupDirective);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormGroupDirective,
		selectors: [[
			"",
			"formGroup",
			""
		]],
		hostBindings: function FormGroupDirective_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("submit", function FormGroupDirective_submit_HostBindingHandler($event) {
				return ctx.onSubmit($event);
			})("reset", function FormGroupDirective_reset_HostBindingHandler() {
				return ctx.onReset();
			});
		},
		inputs: { form: [
			0,
			"formGroup",
			"form"
		] },
		outputs: { ngSubmit: "ngSubmit" },
		exportAs: ["ngForm"],
		standalone: false,
		features: [ɵɵProvidersFeature([formDirectiveProvider$1]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormGroupDirective, [{
		type: Directive,
		args: [{
			selector: "[formGroup]",
			providers: [formDirectiveProvider$1],
			host: {
				"(submit)": "onSubmit($event)",
				"(reset)": "onReset()"
			},
			exportAs: "ngForm",
			standalone: false
		}]
	}], null, {
		form: [{
			type: Input,
			args: ["formGroup"]
		}],
		ngSubmit: [{ type: Output }]
	});
})();
var formControlBinding$1 = {
	provide: NgControl,
	useExisting: forwardRef(() => NgModel)
};
var resolvedPromise = (() => Promise.resolve())();
var NgModel = class NgModel extends NgControl {
	_changeDetectorRef;
	callSetDisabledState;
	control = new FormControl();
	static ngAcceptInputType_isDisabled;
	_registered = false;
	_ngModelInjector;
	viewModel;
	name = "";
	isDisabled;
	model;
	options;
	update = new EventEmitter();
	constructor(parent, validators, asyncValidators, valueAccessors, _changeDetectorRef, callSetDisabledState, injector, renderer) {
		super(injector, renderer, valueAccessors);
		this._changeDetectorRef = _changeDetectorRef;
		this.callSetDisabledState = callSetDisabledState;
		this._parent = parent;
		if (typeof ngDevMode === "undefined" || ngDevMode) this._ngModelInjector = injector;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	ngOnChanges(changes) {
		if (!this._registered && (typeof ngDevMode === "undefined" || ngDevMode) && this._parent === null && !this.options?.standalone) {
			const parentContainer = this._ngModelInjector?.get(ControlContainer, null);
			if (parentContainer != null) {
				const typeName = parentContainer instanceof NgForm ? "NgForm" : parentContainer instanceof FormGroupDirective ? "FormGroupDirective" : parentContainer instanceof NgModelGroup ? "NgModelGroup" : parentContainer.constructor.name || "ControlContainer";
				console.warn(ngModelInChildComponentWarning(typeName));
			}
		}
		this._checkForErrors();
		if (!this._registered || "name" in changes) {
			if (this._registered) {
				this._checkName();
				if (this.formDirective) {
					const oldName = changes["name"].previousValue;
					this.formDirective.removeControl({
						name: oldName,
						path: this._getPath(oldName)
					});
				}
			}
			this._setUpControl();
		}
		if ("isDisabled" in changes) this._updateDisabled(changes);
		if (isPropertyUpdated(changes, this.viewModel)) {
			this._updateValue(this.model);
			this.viewModel = this.model;
		}
	}
	ngOnDestroy() {
		this.formDirective?.removeControl(this);
	}
	ɵngControlCreate(host) {
		super.ngControlCreate(host);
	}
	ɵngControlUpdate(host) {
		super.ngControlUpdate(host, false);
	}
	get shouldBindRequired() {
		return false;
	}
	get path() {
		return this._getPath(this.name);
	}
	get formDirective() {
		return this._parent ? this._parent.formDirective : null;
	}
	viewToModelUpdate(newValue) {
		this.viewModel = newValue;
		this.update.emit(newValue);
	}
	_setUpControl() {
		this._setUpdateStrategy();
		this._isStandalone() ? this._setUpStandalone() : this.formDirective.addControl(this);
		this._registered = true;
	}
	_setUpdateStrategy() {
		if (this.options && this.options.updateOn != null) this.control._updateOn = this.options.updateOn;
	}
	_isStandalone() {
		return !this._parent || !!(this.options && this.options.standalone);
	}
	_setUpStandalone() {
		if (!this.isCustomControlBased) {
			this.valueAccessor ??= this.selectedValueAccessor;
			setUpControlValueAccessor(this.control, this, this.callSetDisabledState);
		} else this.setupCustomControl();
		this.control.updateValueAndValidity({ emitEvent: false });
	}
	_setupWithForm(callSetDisabledState) {
		if (!this.isCustomControlBased) {
			this.valueAccessor ??= this.selectedValueAccessor;
			setUpControlValueAccessor(this.control, this, callSetDisabledState);
		} else this.setupCustomControl();
	}
	_checkForErrors() {
		if ((typeof ngDevMode === "undefined" || ngDevMode) && !this._isStandalone()) checkParentType$1(this._parent);
		this._checkName();
	}
	_checkName() {
		if (this.options && this.options.name) this.name = this.options.name;
		if (!this._isStandalone() && !this.name && (typeof ngDevMode === "undefined" || ngDevMode)) throw missingNameException();
	}
	_updateValue(value) {
		resolvedPromise.then(() => {
			this.control.setValue(value, { emitViewToModelChange: false });
			this._changeDetectorRef?.markForCheck();
		});
	}
	_updateDisabled(changes) {
		const disabledValue = changes["isDisabled"].currentValue;
		const isDisabled = disabledValue !== 0 && booleanAttribute(disabledValue);
		resolvedPromise.then(() => {
			if (isDisabled && !this.control.disabled) this.control.disable();
			else if (!isDisabled && this.control.disabled) this.control.enable();
			this._changeDetectorRef?.markForCheck();
		});
	}
	_getPath(controlName) {
		return this._parent ? controlPath(controlName, this._parent) : [controlName];
	}
	static ɵfac = function NgModel_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgModel)(ɵɵdirectiveInject(ControlContainer, 9), ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10), ɵɵdirectiveInject(NG_VALUE_ACCESSOR, 10), ɵɵdirectiveInject(ChangeDetectorRef, 8), ɵɵdirectiveInject(CALL_SET_DISABLED_STATE, 8), ɵɵdirectiveInject(Injector, 8), ɵɵdirectiveInject(Renderer2, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgModel,
		selectors: [[
			"",
			"ngModel",
			"",
			3,
			"formControlName",
			"",
			3,
			"formControl",
			""
		]],
		inputs: {
			name: "name",
			isDisabled: [
				0,
				"disabled",
				"isDisabled"
			],
			model: [
				0,
				"ngModel",
				"model"
			],
			options: [
				0,
				"ngModelOptions",
				"options"
			]
		},
		outputs: { update: "ngModelChange" },
		exportAs: ["ngModel"],
		standalone: false,
		features: [
			ɵɵProvidersFeature([formControlBinding$1, NG_CONTROL_INTEGRATION_PROVIDER]),
			ɵɵInheritDefinitionFeature,
			ɵɵNgOnChangesFeature,
			ɵɵControlFeature(null)
		]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgModel, [{
		type: Directive,
		args: [{
			selector: "[ngModel]:not([formControlName]):not([formControl])",
			providers: [formControlBinding$1, NG_CONTROL_INTEGRATION_PROVIDER],
			exportAs: "ngModel",
			standalone: false
		}]
	}], () => [
		{
			type: ControlContainer,
			decorators: [{ type: Optional }, { type: Host }]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALUE_ACCESSOR]
				}
			]
		},
		{
			type: ChangeDetectorRef,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [ChangeDetectorRef]
			}]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [CALL_SET_DISABLED_STATE]
			}]
		},
		{
			type: Injector,
			decorators: [{ type: Optional }]
		},
		{
			type: Renderer2,
			decorators: [{ type: Optional }]
		}
	], {
		name: [{ type: Input }],
		isDisabled: [{
			type: Input,
			args: ["disabled"]
		}],
		model: [{
			type: Input,
			args: ["ngModel"]
		}],
		options: [{
			type: Input,
			args: ["ngModelOptions"]
		}],
		update: [{
			type: Output,
			args: ["ngModelChange"]
		}]
	});
})();
function checkParentType$1(parent) {
	if (!(parent instanceof NgModelGroup) && parent instanceof AbstractFormGroupDirective) throw formGroupNameException();
	else if (!(parent instanceof NgModelGroup) && !(parent instanceof NgForm)) throw modelParentException();
}
var ɵNgNoValidate = class ɵNgNoValidate {
	static ɵfac = function ɵNgNoValidate_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ɵNgNoValidate)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: ɵNgNoValidate,
		selectors: [[
			"form",
			3,
			"ngNoForm",
			"",
			3,
			"ngNativeValidate",
			""
		]],
		hostAttrs: ["novalidate", ""],
		standalone: false
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ɵNgNoValidate, [{
		type: Directive,
		args: [{
			selector: "form:not([ngNoForm]):not([ngNativeValidate])",
			host: { "novalidate": "" },
			standalone: false
		}]
	}], null, null);
})();
var NUMBER_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => NumberValueAccessor),
	multi: true
};
var NumberValueAccessor = class NumberValueAccessor extends BuiltInControlValueAccessor {
	writeValue(value) {
		const normalizedValue = value == null ? "" : value;
		this.setProperty("value", normalizedValue);
	}
	registerOnChange(fn) {
		this.onChange = (value) => {
			fn(value == "" ? null : parseFloat(value));
		};
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵNumberValueAccessor_BaseFactory = void 0;
		return function NumberValueAccessor_Factory(__ngFactoryType__) {
			return (ɵNumberValueAccessor_BaseFactory || (ɵNumberValueAccessor_BaseFactory = ɵɵgetInheritedFactory(NumberValueAccessor)))(__ngFactoryType__ || NumberValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NumberValueAccessor,
		selectors: [
			[
				"input",
				"type",
				"number",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"number",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"number",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function NumberValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("input", function NumberValueAccessor_input_HostBindingHandler($event) {
				return ctx.onChange($event.target.value);
			})("blur", function NumberValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		standalone: false,
		features: [ɵɵProvidersFeature([NUMBER_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NumberValueAccessor, [{
		type: Directive,
		args: [{
			selector: "input[type=number]:not([ngNoCva])[formControlName],input[type=number]:not([ngNoCva])[formControl],input[type=number]:not([ngNoCva])[ngModel]",
			host: {
				"(input)": "onChange($any($event.target).value)",
				"(blur)": "onTouched()"
			},
			providers: [NUMBER_VALUE_ACCESSOR],
			standalone: false
		}]
	}], null, null);
})();
var RADIO_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => RadioControlValueAccessor),
	multi: true
};
function throwNameError() {
	throw new RuntimeError(1202, `
      If you define both a name and a formControlName attribute on your radio button, their values
      must match. Ex: <input type="radio" formControlName="food" name="food">
    `);
}
var RadioControlRegistry = class RadioControlRegistry {
	_accessors = [];
	add(control, accessor) {
		this._accessors.push([control, accessor]);
	}
	remove(accessor) {
		for (let i = this._accessors.length - 1; i >= 0; --i) if (this._accessors[i][1] === accessor) {
			this._accessors.splice(i, 1);
			return;
		}
	}
	select(accessor) {
		this._accessors.forEach((c) => {
			if (this._isSameGroup(c, accessor) && c[1] !== accessor) c[1].fireUncheck(accessor.value);
		});
	}
	_isSameGroup(controlPair, accessor) {
		if (!controlPair[0].control) return false;
		return controlPair[0]._parent === accessor._control._parent && controlPair[1].name === accessor.name;
	}
	static ɵfac = function RadioControlRegistry_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || RadioControlRegistry)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: RadioControlRegistry,
		factory: RadioControlRegistry.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(RadioControlRegistry, [{ type: Service }], null, null);
})();
var RadioControlValueAccessor = class RadioControlValueAccessor extends BuiltInControlValueAccessor {
	_registry;
	_injector;
	_state;
	_control;
	_fn;
	setDisabledStateFired = false;
	onChange = () => {};
	name;
	formControlName;
	value;
	callSetDisabledState = inject(CALL_SET_DISABLED_STATE, { optional: true }) ?? setDisabledStateDefault;
	constructor(renderer, elementRef, _registry, _injector) {
		super(renderer, elementRef);
		this._registry = _registry;
		this._injector = _injector;
	}
	ngOnChanges(changes) {
		const control = this._control?.control;
		if (changes["value"] && control) this.writeValue(control.value);
	}
	ngOnInit() {
		this._control = this._injector.get(NgControl);
		this._checkName();
		this._registry.add(this._control, this);
	}
	ngOnDestroy() {
		this._registry.remove(this);
	}
	writeValue(value) {
		this._state = value === this.value;
		this.setProperty("checked", this._state);
	}
	registerOnChange(fn) {
		this._fn = fn;
		this.onChange = () => {
			fn(this.value);
			this._registry.select(this);
		};
	}
	setDisabledState(isDisabled) {
		if (this.setDisabledStateFired || isDisabled || this.callSetDisabledState === "whenDisabledForLegacyCode") this.setProperty("disabled", isDisabled);
		this.setDisabledStateFired = true;
	}
	fireUncheck(value) {
		this.writeValue(value);
	}
	_checkName() {
		if (this.name && this.formControlName && this.name !== this.formControlName && (typeof ngDevMode === "undefined" || ngDevMode)) throwNameError();
		if (!this.name && this.formControlName) this.name = this.formControlName;
	}
	static ɵfac = function RadioControlValueAccessor_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || RadioControlValueAccessor)(ɵɵdirectiveInject(Renderer2), ɵɵdirectiveInject(ElementRef), ɵɵdirectiveInject(RadioControlRegistry), ɵɵdirectiveInject(Injector));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: RadioControlValueAccessor,
		selectors: [
			[
				"input",
				"type",
				"radio",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"radio",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"radio",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function RadioControlValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("change", function RadioControlValueAccessor_change_HostBindingHandler() {
				return ctx.onChange();
			})("blur", function RadioControlValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		inputs: {
			name: "name",
			formControlName: "formControlName",
			value: "value"
		},
		standalone: false,
		features: [
			ɵɵProvidersFeature([RADIO_VALUE_ACCESSOR]),
			ɵɵInheritDefinitionFeature,
			ɵɵNgOnChangesFeature
		]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(RadioControlValueAccessor, [{
		type: Directive,
		args: [{
			selector: "input[type=radio]:not([ngNoCva])[formControlName],input[type=radio]:not([ngNoCva])[formControl],input[type=radio]:not([ngNoCva])[ngModel]",
			host: {
				"(change)": "onChange()",
				"(blur)": "onTouched()"
			},
			providers: [RADIO_VALUE_ACCESSOR],
			standalone: false
		}]
	}], () => [
		{ type: Renderer2 },
		{ type: ElementRef },
		{ type: RadioControlRegistry },
		{ type: Injector }
	], {
		name: [{ type: Input }],
		formControlName: [{ type: Input }],
		value: [{ type: Input }]
	});
})();
var RANGE_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => RangeValueAccessor),
	multi: true
};
var RangeValueAccessor = class RangeValueAccessor extends BuiltInControlValueAccessor {
	writeValue(value) {
		this.setProperty("value", parseFloat(value));
	}
	registerOnChange(fn) {
		this.onChange = (value) => {
			fn(value == "" ? null : parseFloat(value));
		};
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵRangeValueAccessor_BaseFactory = void 0;
		return function RangeValueAccessor_Factory(__ngFactoryType__) {
			return (ɵRangeValueAccessor_BaseFactory || (ɵRangeValueAccessor_BaseFactory = ɵɵgetInheritedFactory(RangeValueAccessor)))(__ngFactoryType__ || RangeValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: RangeValueAccessor,
		selectors: [
			[
				"input",
				"type",
				"range",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"range",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"input",
				"type",
				"range",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function RangeValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("change", function RangeValueAccessor_change_HostBindingHandler($event) {
				return ctx.onChange($event.target.value);
			})("input", function RangeValueAccessor_input_HostBindingHandler($event) {
				return ctx.onChange($event.target.value);
			})("blur", function RangeValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		standalone: false,
		features: [ɵɵProvidersFeature([RANGE_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(RangeValueAccessor, [{
		type: Directive,
		args: [{
			selector: "input[type=range]:not([ngNoCva])[formControlName],input[type=range]:not([ngNoCva])[formControl],input[type=range]:not([ngNoCva])[ngModel]",
			host: {
				"(change)": "onChange($any($event.target).value)",
				"(input)": "onChange($any($event.target).value)",
				"(blur)": "onTouched()"
			},
			providers: [RANGE_VALUE_ACCESSOR],
			standalone: false
		}]
	}], null, null);
})();
var FormArray = class extends AbstractControl {
	constructor(controls, validatorOrOpts, asyncValidator) {
		super(pickValidators(validatorOrOpts), pickAsyncValidators(asyncValidator, validatorOrOpts));
		this.controls = controls;
		this._initObservables();
		this._setUpdateStrategy(validatorOrOpts);
		this._setUpControls();
		this.updateValueAndValidity({
			onlySelf: true,
			emitEvent: !!this.asyncValidator
		});
	}
	controls;
	at(index) {
		return this.controls[this._adjustIndex(index)];
	}
	push(control, options = {}) {
		if (Array.isArray(control)) control.forEach((ctrl) => {
			this.controls.push(ctrl);
			this._registerControl(ctrl);
		});
		else {
			this.controls.push(control);
			this._registerControl(control);
		}
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
		this._onCollectionChange();
	}
	insert(index, control, options = {}) {
		this.controls.splice(index, 0, control);
		this._registerControl(control);
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
	}
	removeAt(index, options = {}) {
		let adjustedIndex = this._adjustIndex(index);
		if (adjustedIndex < 0) adjustedIndex = 0;
		if (this.controls[adjustedIndex]) this.controls[adjustedIndex]._registerOnCollectionChange(() => {});
		this.controls.splice(adjustedIndex, 1);
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
	}
	setControl(index, control, options = {}) {
		let adjustedIndex = this._adjustIndex(index);
		if (adjustedIndex < 0) adjustedIndex = 0;
		if (this.controls[adjustedIndex]) this.controls[adjustedIndex]._registerOnCollectionChange(() => {});
		this.controls.splice(adjustedIndex, 1);
		if (control) {
			this.controls.splice(adjustedIndex, 0, control);
			this._registerControl(control);
		}
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
		this._onCollectionChange();
	}
	get length() {
		return this.controls.length;
	}
	setValue(value, options = {}) {
		untracked(() => {
			assertAllValuesPresent(this, false, value);
			value.forEach((newValue, index) => {
				assertControlPresent(this, false, index);
				this.at(index).setValue(newValue, {
					onlySelf: true,
					emitEvent: options.emitEvent
				});
			});
			this.updateValueAndValidity(options);
		});
	}
	patchValue(value, options = {}) {
		if (value == null) return;
		value.forEach((newValue, index) => {
			if (this.at(index)) this.at(index).patchValue(newValue, {
				onlySelf: true,
				emitEvent: options.emitEvent
			});
		});
		this.updateValueAndValidity(options);
	}
	reset(value = [], options = {}) {
		this._forEachChild((control, index) => {
			control.reset(value[index], {
				...options,
				onlySelf: true
			});
		});
		this._updatePristine(options, this);
		this._updateTouched(options, this);
		this.updateValueAndValidity(options);
		if (options?.emitEvent !== false) this._events.next(new FormResetEvent(this));
	}
	getRawValue() {
		return this.controls.map((control) => control.getRawValue());
	}
	clear(options = {}) {
		if (this.controls.length < 1) return;
		this._forEachChild((control) => control._registerOnCollectionChange(() => {}));
		this.controls.splice(0);
		this.updateValueAndValidity({ emitEvent: options.emitEvent });
	}
	_adjustIndex(index) {
		return index < 0 ? index + this.length : index;
	}
	_syncPendingControls() {
		let subtreeUpdated = this.controls.reduce((updated, child) => {
			return child._syncPendingControls() ? true : updated;
		}, false);
		if (subtreeUpdated) this.updateValueAndValidity({ onlySelf: true });
		return subtreeUpdated;
	}
	_forEachChild(cb) {
		this.controls.forEach((control, index) => {
			cb(control, index);
		});
	}
	_updateValue() {
		this.value = this.controls.filter((control) => control.enabled || this.disabled).map((control) => control.value);
	}
	_anyControls(condition) {
		return this.controls.some((control) => control.enabled && condition(control));
	}
	_setUpControls() {
		this._forEachChild((control) => this._registerControl(control));
	}
	_allControlsDisabled() {
		for (const control of this.controls) if (control.enabled) return false;
		return this.controls.length > 0 || this.disabled;
	}
	_registerControl(control) {
		control.setParent(this);
		control._registerOnCollectionChange(this._onCollectionChange);
	}
	_find(name) {
		return this.at(name) ?? null;
	}
};
var formDirectiveProvider = {
	provide: ControlContainer,
	useExisting: forwardRef(() => FormArrayDirective)
};
var FormArrayDirective = class FormArrayDirective extends AbstractFormDirective {
	form = null;
	ngSubmit = new EventEmitter();
	get control() {
		return this.form;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵFormArrayDirective_BaseFactory = void 0;
		return function FormArrayDirective_Factory(__ngFactoryType__) {
			return (ɵFormArrayDirective_BaseFactory || (ɵFormArrayDirective_BaseFactory = ɵɵgetInheritedFactory(FormArrayDirective)))(__ngFactoryType__ || FormArrayDirective);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormArrayDirective,
		selectors: [[
			"",
			"formArray",
			""
		]],
		hostBindings: function FormArrayDirective_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("submit", function FormArrayDirective_submit_HostBindingHandler($event) {
				return ctx.onSubmit($event);
			})("reset", function FormArrayDirective_reset_HostBindingHandler() {
				return ctx.onReset();
			});
		},
		inputs: { form: [
			0,
			"formArray",
			"form"
		] },
		outputs: { ngSubmit: "ngSubmit" },
		exportAs: ["ngForm"],
		standalone: false,
		features: [ɵɵProvidersFeature([formDirectiveProvider]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormArrayDirective, [{
		type: Directive,
		args: [{
			selector: "[formArray]",
			providers: [formDirectiveProvider],
			host: {
				"(submit)": "onSubmit($event)",
				"(reset)": "onReset()"
			},
			exportAs: "ngForm",
			standalone: false
		}]
	}], null, {
		form: [{
			type: Input,
			args: ["formArray"]
		}],
		ngSubmit: [{ type: Output }]
	});
})();
var NG_MODEL_WITH_FORM_CONTROL_WARNING = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "NgModelWithFormControlWarning" : "");
var formControlBinding = {
	provide: NgControl,
	useExisting: forwardRef(() => FormControlDirective)
};
var FormControlDirective = class FormControlDirective extends NgControl {
	_ngModelWarningConfig;
	callSetDisabledState;
	viewModel;
	form;
	set isDisabled(isDisabled) {
		if (typeof ngDevMode === "undefined" || ngDevMode) console.warn(disabledAttrWarning);
	}
	model;
	update = new EventEmitter();
	static _ngModelWarningSentOnce = false;
	_ngModelWarningSent = false;
	constructor(validators, asyncValidators, valueAccessors, _ngModelWarningConfig, callSetDisabledState, renderer, injector) {
		super(injector, renderer, valueAccessors);
		this._ngModelWarningConfig = _ngModelWarningConfig;
		this.callSetDisabledState = callSetDisabledState;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	ngOnChanges(changes) {
		if (this._isControlChanged(changes)) {
			const previousForm = changes["form"].previousValue;
			if (previousForm) {
				cleanUpControl(previousForm, this, false);
				this.removeParseErrorsValidator(previousForm);
			}
			if (!this.isCustomControlBased) {
				this.valueAccessor ??= this.selectedValueAccessor;
				setUpControlValueAccessor(this.form, this, this.callSetDisabledState);
			} else this.setupCustomControl();
			this.form.updateValueAndValidity({ emitEvent: false });
		}
		if (isPropertyUpdated(changes, this.viewModel)) {
			if (typeof ngDevMode === "undefined" || ngDevMode) _ngModelWarning("formControl", FormControlDirective, this, this._ngModelWarningConfig);
			this.form.setValue(this.model);
			this.viewModel = this.model;
		}
	}
	ngOnDestroy() {
		if (this.form) cleanUpControl(this.form, this, false);
	}
	get path() {
		return [];
	}
	get control() {
		return this.form;
	}
	viewToModelUpdate(newValue) {
		this.viewModel = newValue;
		this.update.emit(newValue);
	}
	_isControlChanged(changes) {
		return Object.hasOwn(changes, "form");
	}
	ɵngControlCreate(host) {
		super.ngControlCreate(host);
	}
	ɵngControlUpdate(host) {
		super.ngControlUpdate(host, true);
	}
	static ɵfac = function FormControlDirective_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormControlDirective)(ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10), ɵɵdirectiveInject(NG_VALUE_ACCESSOR, 10), ɵɵdirectiveInject(NG_MODEL_WITH_FORM_CONTROL_WARNING, 8), ɵɵdirectiveInject(CALL_SET_DISABLED_STATE, 8), ɵɵdirectiveInject(Renderer2, 8), ɵɵdirectiveInject(Injector, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormControlDirective,
		selectors: [[
			"",
			"formControl",
			""
		]],
		inputs: {
			form: [
				0,
				"formControl",
				"form"
			],
			isDisabled: [
				0,
				"disabled",
				"isDisabled"
			],
			model: [
				0,
				"ngModel",
				"model"
			]
		},
		outputs: { update: "ngModelChange" },
		exportAs: ["ngForm"],
		standalone: false,
		features: [
			ɵɵProvidersFeature([formControlBinding, NG_CONTROL_INTEGRATION_PROVIDER]),
			ɵɵInheritDefinitionFeature,
			ɵɵNgOnChangesFeature,
			ɵɵControlFeature(null)
		]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormControlDirective, [{
		type: Directive,
		args: [{
			selector: "[formControl]",
			providers: [formControlBinding, NG_CONTROL_INTEGRATION_PROVIDER],
			exportAs: "ngForm",
			standalone: false
		}]
	}], () => [
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALUE_ACCESSOR]
				}
			]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [NG_MODEL_WITH_FORM_CONTROL_WARNING]
			}]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [CALL_SET_DISABLED_STATE]
			}]
		},
		{
			type: Renderer2,
			decorators: [{ type: Optional }]
		},
		{
			type: Injector,
			decorators: [{ type: Optional }]
		}
	], {
		form: [{
			type: Input,
			args: ["formControl"]
		}],
		isDisabled: [{
			type: Input,
			args: ["disabled"]
		}],
		model: [{
			type: Input,
			args: ["ngModel"]
		}],
		update: [{
			type: Output,
			args: ["ngModelChange"]
		}]
	});
})();
var formGroupNameProvider = {
	provide: ControlContainer,
	useExisting: forwardRef(() => FormGroupName)
};
var FormGroupName = class FormGroupName extends AbstractFormGroupDirective {
	name = null;
	constructor(parent, validators, asyncValidators) {
		super();
		this._parent = parent;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	_checkParentType() {
		if (hasInvalidParent(this._parent) && (typeof ngDevMode === "undefined" || ngDevMode)) throw groupParentException();
	}
	static ɵfac = function FormGroupName_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormGroupName)(ɵɵdirectiveInject(ControlContainer, 13), ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormGroupName,
		selectors: [[
			"",
			"formGroupName",
			""
		]],
		inputs: { name: [
			0,
			"formGroupName",
			"name"
		] },
		standalone: false,
		features: [ɵɵProvidersFeature([formGroupNameProvider]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormGroupName, [{
		type: Directive,
		args: [{
			selector: "[formGroupName]",
			providers: [formGroupNameProvider],
			standalone: false
		}]
	}], () => [
		{
			type: ControlContainer,
			decorators: [
				{ type: Optional },
				{ type: Host },
				{ type: SkipSelf }
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		}
	], { name: [{
		type: Input,
		args: ["formGroupName"]
	}] });
})();
var formArrayNameProvider = {
	provide: ControlContainer,
	useExisting: forwardRef(() => FormArrayName)
};
var FormArrayName = class FormArrayName extends ControlContainer {
	_parent;
	name = null;
	constructor(parent, validators, asyncValidators) {
		super();
		this._parent = parent;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	ngOnInit() {
		if (hasInvalidParent(this._parent) && (typeof ngDevMode === "undefined" || ngDevMode)) throw arrayParentException();
		this.formDirective.addFormArray(this);
	}
	ngOnDestroy() {
		this.formDirective?.removeFormArray(this);
	}
	get control() {
		return this.formDirective.getFormArray(this);
	}
	get formDirective() {
		return this._parent ? this._parent.formDirective : null;
	}
	get path() {
		return controlPath(this.name == null ? this.name : this.name.toString(), this._parent);
	}
	static ɵfac = function FormArrayName_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormArrayName)(ɵɵdirectiveInject(ControlContainer, 13), ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormArrayName,
		selectors: [[
			"",
			"formArrayName",
			""
		]],
		inputs: { name: [
			0,
			"formArrayName",
			"name"
		] },
		standalone: false,
		features: [ɵɵProvidersFeature([formArrayNameProvider]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormArrayName, [{
		type: Directive,
		args: [{
			selector: "[formArrayName]",
			providers: [formArrayNameProvider],
			standalone: false
		}]
	}], () => [
		{
			type: ControlContainer,
			decorators: [
				{ type: Optional },
				{ type: Host },
				{ type: SkipSelf }
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		}
	], { name: [{
		type: Input,
		args: ["formArrayName"]
	}] });
})();
function hasInvalidParent(parent) {
	return !(parent instanceof FormGroupName) && !(parent instanceof AbstractFormDirective) && !(parent instanceof FormArrayName);
}
var controlNameBinding = {
	provide: NgControl,
	useExisting: forwardRef(() => FormControlName)
};
var FormControlName = class FormControlName extends NgControl {
	_ngModelWarningConfig;
	_added = false;
	viewModel;
	control;
	name = null;
	set isDisabled(isDisabled) {
		if (typeof ngDevMode === "undefined" || ngDevMode) console.warn(disabledAttrWarning);
	}
	model;
	update = new EventEmitter();
	static _ngModelWarningSentOnce = false;
	_ngModelWarningSent = false;
	constructor(parent, validators, asyncValidators, valueAccessors, _ngModelWarningConfig, renderer, injector) {
		super(injector, renderer, valueAccessors);
		this._ngModelWarningConfig = _ngModelWarningConfig;
		this._parent = parent;
		this._setValidators(validators);
		this._setAsyncValidators(asyncValidators);
	}
	_setupWithForm(control, callSetDisabledState) {
		this.control = control;
		if (!this.isCustomControlBased) {
			this.valueAccessor ??= this.selectedValueAccessor;
			setUpControlValueAccessor(control, this, callSetDisabledState);
		} else this.setupCustomControl();
	}
	ngOnChanges(changes) {
		if (!this._added) this._setUpControl();
		if (isPropertyUpdated(changes, this.viewModel)) {
			if (typeof ngDevMode === "undefined" || ngDevMode) _ngModelWarning("formControlName", FormControlName, this, this._ngModelWarningConfig);
			this.viewModel = this.model;
			this.formDirective.updateModel(this, this.model);
		}
	}
	ngOnDestroy() {
		this.formDirective?.removeControl(this);
	}
	viewToModelUpdate(newValue) {
		this.viewModel = newValue;
		this.update.emit(newValue);
	}
	get path() {
		return controlPath(this.name == null ? this.name : this.name.toString(), this._parent);
	}
	get formDirective() {
		return this._parent ? this._parent.formDirective : null;
	}
	_setUpControl() {
		if (typeof ngDevMode === "undefined" || ngDevMode) checkParentType(this._parent, this.name);
		this.control = this.formDirective.addControl(this);
		this._added = true;
	}
	ɵngControlCreate(host) {
		super.ngControlCreate(host);
	}
	ɵngControlUpdate(host) {
		if (!this.isCustomControlBased) return;
		if (!this._added) this._setUpControl();
		super.ngControlUpdate(host, true);
	}
	static ɵfac = function FormControlName_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormControlName)(ɵɵdirectiveInject(ControlContainer, 13), ɵɵdirectiveInject(NG_VALIDATORS, 10), ɵɵdirectiveInject(NG_ASYNC_VALIDATORS, 10), ɵɵdirectiveInject(NG_VALUE_ACCESSOR, 10), ɵɵdirectiveInject(NG_MODEL_WITH_FORM_CONTROL_WARNING, 8), ɵɵdirectiveInject(Renderer2, 8), ɵɵdirectiveInject(Injector, 8));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormControlName,
		selectors: [[
			"",
			"formControlName",
			""
		]],
		inputs: {
			name: [
				0,
				"formControlName",
				"name"
			],
			isDisabled: [
				0,
				"disabled",
				"isDisabled"
			],
			model: [
				0,
				"ngModel",
				"model"
			]
		},
		outputs: { update: "ngModelChange" },
		standalone: false,
		features: [
			ɵɵProvidersFeature([controlNameBinding, NG_CONTROL_INTEGRATION_PROVIDER]),
			ɵɵInheritDefinitionFeature,
			ɵɵNgOnChangesFeature,
			ɵɵControlFeature(null)
		]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormControlName, [{
		type: Directive,
		args: [{
			selector: "[formControlName]",
			providers: [controlNameBinding, NG_CONTROL_INTEGRATION_PROVIDER],
			standalone: false
		}]
	}], () => [
		{
			type: ControlContainer,
			decorators: [
				{ type: Optional },
				{ type: Host },
				{ type: SkipSelf }
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_ASYNC_VALIDATORS]
				}
			]
		},
		{
			type: void 0,
			decorators: [
				{ type: Optional },
				{ type: Self },
				{
					type: Inject,
					args: [NG_VALUE_ACCESSOR]
				}
			]
		},
		{
			type: void 0,
			decorators: [{ type: Optional }, {
				type: Inject,
				args: [NG_MODEL_WITH_FORM_CONTROL_WARNING]
			}]
		},
		{
			type: Renderer2,
			decorators: [{ type: Optional }]
		},
		{
			type: Injector,
			decorators: [{ type: Optional }]
		}
	], {
		name: [{
			type: Input,
			args: ["formControlName"]
		}],
		isDisabled: [{
			type: Input,
			args: ["disabled"]
		}],
		model: [{
			type: Input,
			args: ["ngModel"]
		}],
		update: [{
			type: Output,
			args: ["ngModelChange"]
		}]
	});
})();
function checkParentType(parent, name) {
	if (!(parent instanceof FormGroupName) && parent instanceof AbstractFormGroupDirective) throw ngModelGroupException();
	else if (!(parent instanceof FormGroupName) && !(parent instanceof AbstractFormDirective) && !(parent instanceof FormArrayName)) throw controlParentException(name);
}
var SELECT_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => SelectControlValueAccessor),
	multi: true
};
function _buildValueString$1(id, value) {
	if (id == null) return `${value}`;
	if (value && typeof value === "object") value = "Object";
	return `${id}: ${value}`.slice(0, 50);
}
function _extractId$1(valueString) {
	return valueString.split(":")[0];
}
var SelectControlValueAccessor = class SelectControlValueAccessor extends BuiltInControlValueAccessor {
	value;
	_optionMap = /* @__PURE__ */ new Map();
	_idCounter = 0;
	set compareWith(fn) {
		if (typeof fn !== "function" && (typeof ngDevMode === "undefined" || ngDevMode)) throw new RuntimeError(1201, `compareWith must be a function, but received ${JSON.stringify(fn)}`);
		this._compareWith = fn;
	}
	_compareWith = Object.is;
	appRefInjector = inject(ApplicationRef).injector;
	destroyRef = inject(DestroyRef);
	cdr = inject(ChangeDetectorRef);
	_queuedWrite = false;
	_writeValueAfterRender() {
		if (this._queuedWrite || this.appRefInjector.destroyed) return;
		this._queuedWrite = true;
		afterNextRender({ write: () => {
			if (this.destroyRef.destroyed) return;
			this._queuedWrite = false;
			this.writeValue(this.value);
		} }, { injector: this.appRefInjector });
	}
	writeValue(value) {
		this.cdr.markForCheck();
		this.value = value;
		const valueString = _buildValueString$1(this._getOptionId(value), value);
		this.setProperty("value", valueString);
	}
	registerOnChange(fn) {
		this.onChange = (valueString) => {
			this.value = this._getOptionValue(valueString);
			fn(this.value);
		};
	}
	_registerOption() {
		return (this._idCounter++).toString();
	}
	_getOptionId(value) {
		for (const id of this._optionMap.keys()) if (this._compareWith(this._optionMap.get(id), value)) return id;
		return null;
	}
	_getOptionValue(valueString) {
		const id = _extractId$1(valueString);
		return this._optionMap.has(id) ? this._optionMap.get(id) : valueString;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵSelectControlValueAccessor_BaseFactory = void 0;
		return function SelectControlValueAccessor_Factory(__ngFactoryType__) {
			return (ɵSelectControlValueAccessor_BaseFactory || (ɵSelectControlValueAccessor_BaseFactory = ɵɵgetInheritedFactory(SelectControlValueAccessor)))(__ngFactoryType__ || SelectControlValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: SelectControlValueAccessor,
		selectors: [
			[
				"select",
				"formControlName",
				"",
				3,
				"multiple",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"select",
				"formControl",
				"",
				3,
				"multiple",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"select",
				"ngModel",
				"",
				3,
				"multiple",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function SelectControlValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("change", function SelectControlValueAccessor_change_HostBindingHandler($event) {
				return ctx.onChange($event.target.value);
			})("blur", function SelectControlValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		inputs: { compareWith: "compareWith" },
		standalone: false,
		features: [ɵɵProvidersFeature([SELECT_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(SelectControlValueAccessor, [{
		type: Directive,
		args: [{
			selector: "select:not([multiple]):not([ngNoCva])[formControlName],select:not([multiple]):not([ngNoCva])[formControl],select:not([multiple]):not([ngNoCva])[ngModel]",
			host: {
				"(change)": "onChange($any($event.target).value)",
				"(blur)": "onTouched()"
			},
			providers: [SELECT_VALUE_ACCESSOR],
			standalone: false
		}]
	}], null, { compareWith: [{ type: Input }] });
})();
var NgSelectOption = class NgSelectOption {
	_element;
	_renderer;
	_select;
	id;
	constructor(_element, _renderer, _select) {
		this._element = _element;
		this._renderer = _renderer;
		this._select = _select;
		if (this._select) this.id = this._select._registerOption();
	}
	set ngValue(value) {
		if (this._select == null) return;
		this._select._optionMap.set(this.id, value);
		this._setElementValue(_buildValueString$1(this.id, value));
		this._select._writeValueAfterRender();
	}
	set value(value) {
		this._setElementValue(value);
		this._select?._writeValueAfterRender();
	}
	_setElementValue(value) {
		this._renderer.setProperty(this._element.nativeElement, "value", value);
	}
	ngOnDestroy() {
		this._select?._optionMap.delete(this.id);
		this._select?._writeValueAfterRender();
	}
	static ɵfac = function NgSelectOption_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NgSelectOption)(ɵɵdirectiveInject(ElementRef), ɵɵdirectiveInject(Renderer2), ɵɵdirectiveInject(SelectControlValueAccessor, 9));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: NgSelectOption,
		selectors: [["option"]],
		inputs: {
			ngValue: "ngValue",
			value: "value"
		},
		standalone: false
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgSelectOption, [{
		type: Directive,
		args: [{
			selector: "option",
			standalone: false
		}]
	}], () => [
		{ type: ElementRef },
		{ type: Renderer2 },
		{
			type: SelectControlValueAccessor,
			decorators: [{ type: Optional }, { type: Host }]
		}
	], {
		ngValue: [{
			type: Input,
			args: ["ngValue"]
		}],
		value: [{
			type: Input,
			args: ["value"]
		}]
	});
})();
var NEGATIVE_ZERO = Symbol("negativeZero");
var SELECT_MULTIPLE_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => SelectMultipleControlValueAccessor),
	multi: true
};
function _buildValueString(id, value) {
	if (id == null) return `${value}`;
	if (typeof value === "string") value = `'${value}'`;
	if (value && typeof value === "object") value = "Object";
	return `${id}: ${value}`.slice(0, 50);
}
function _extractId(valueString) {
	return valueString.split(":")[0];
}
var SelectMultipleControlValueAccessor = class SelectMultipleControlValueAccessor extends BuiltInControlValueAccessor {
	value;
	_optionMap = /* @__PURE__ */ new Map();
	_idCounter = 0;
	set compareWith(fn) {
		if (typeof fn !== "function" && (typeof ngDevMode === "undefined" || ngDevMode)) throw new RuntimeError(1201, `compareWith must be a function, but received ${JSON.stringify(fn)}`);
		this._compareWith = fn;
	}
	_compareWith = Object.is;
	writeValue(value) {
		this.value = value;
		let optionSelectedStateSetter;
		if (Array.isArray(value)) {
			const ids = this._getOptionIds(value);
			optionSelectedStateSetter = (opt, id) => {
				opt._setSelected(ids.has(id));
			};
		} else optionSelectedStateSetter = (opt) => {
			opt._setSelected(false);
		};
		this._optionMap.forEach(optionSelectedStateSetter);
	}
	registerOnChange(fn) {
		this.onChange = (element) => {
			const selected = [];
			const selectedOptions = element.selectedOptions;
			if (selectedOptions !== void 0) {
				const options = selectedOptions;
				for (let i = 0; i < options.length; i++) {
					const opt = options[i];
					const val = this._getOptionValue(opt.value);
					selected.push(val);
				}
			} else {
				const options = element.options;
				for (let i = 0; i < options.length; i++) {
					const opt = options[i];
					if (opt.selected) {
						const val = this._getOptionValue(opt.value);
						selected.push(val);
					}
				}
			}
			this.value = selected;
			fn(selected);
		};
	}
	_registerOption(value) {
		const id = (this._idCounter++).toString();
		this._optionMap.set(id, value);
		return id;
	}
	_getOptionIds(values) {
		if (this._compareWith !== Object.is || values.length < 2) return new Set(values.map((value) => this._getOptionId(value)).filter((id) => id !== null));
		const remainingValues = /* @__PURE__ */ new Set();
		values.forEach((value) => remainingValues.add(value === 0 && Object.is(value, -0) ? NEGATIVE_ZERO : value));
		const ids = /* @__PURE__ */ new Set();
		for (const [id, option] of this._optionMap) {
			if (remainingValues.size === 0) break;
			const optionValue = option._value;
			const value = optionValue === 0 && Object.is(optionValue, -0) ? NEGATIVE_ZERO : optionValue;
			if (remainingValues.delete(value)) ids.add(id);
		}
		return ids;
	}
	_getOptionId(value) {
		for (const id of this._optionMap.keys()) if (this._compareWith(this._optionMap.get(id)._value, value)) return id;
		return null;
	}
	_getOptionValue(valueString) {
		const id = _extractId(valueString);
		return this._optionMap.has(id) ? this._optionMap.get(id)._value : valueString;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵSelectMultipleControlValueAccessor_BaseFactory = void 0;
		return function SelectMultipleControlValueAccessor_Factory(__ngFactoryType__) {
			return (ɵSelectMultipleControlValueAccessor_BaseFactory || (ɵSelectMultipleControlValueAccessor_BaseFactory = ɵɵgetInheritedFactory(SelectMultipleControlValueAccessor)))(__ngFactoryType__ || SelectMultipleControlValueAccessor);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: SelectMultipleControlValueAccessor,
		selectors: [
			[
				"select",
				"multiple",
				"",
				"formControlName",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"select",
				"multiple",
				"",
				"formControl",
				"",
				3,
				"ngNoCva",
				""
			],
			[
				"select",
				"multiple",
				"",
				"ngModel",
				"",
				3,
				"ngNoCva",
				""
			]
		],
		hostBindings: function SelectMultipleControlValueAccessor_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("change", function SelectMultipleControlValueAccessor_change_HostBindingHandler($event) {
				return ctx.onChange($event.target);
			})("blur", function SelectMultipleControlValueAccessor_blur_HostBindingHandler() {
				return ctx.onTouched();
			});
		},
		inputs: { compareWith: "compareWith" },
		standalone: false,
		features: [ɵɵProvidersFeature([SELECT_MULTIPLE_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(SelectMultipleControlValueAccessor, [{
		type: Directive,
		args: [{
			selector: "select[multiple]:not([ngNoCva])[formControlName],select[multiple]:not([ngNoCva])[formControl],select[multiple]:not([ngNoCva])[ngModel]",
			host: {
				"(change)": "onChange($event.target)",
				"(blur)": "onTouched()"
			},
			providers: [SELECT_MULTIPLE_VALUE_ACCESSOR],
			standalone: false
		}]
	}], null, { compareWith: [{ type: Input }] });
})();
var ɵNgSelectMultipleOption = class ɵNgSelectMultipleOption {
	_element;
	_renderer;
	_select;
	id;
	_value;
	constructor(_element, _renderer, _select) {
		this._element = _element;
		this._renderer = _renderer;
		this._select = _select;
		if (this._select) this.id = this._select._registerOption(this);
	}
	set ngValue(value) {
		if (this._select == null) return;
		this._value = value;
		this._setElementValue(_buildValueString(this.id, value));
		this._select.writeValue(this._select.value);
	}
	set value(value) {
		if (this._select) {
			this._value = value;
			this._setElementValue(_buildValueString(this.id, value));
			this._select.writeValue(this._select.value);
		} else this._setElementValue(value);
	}
	_setElementValue(value) {
		this._renderer.setProperty(this._element.nativeElement, "value", value);
	}
	_setSelected(selected) {
		this._renderer.setProperty(this._element.nativeElement, "selected", selected);
	}
	ngOnDestroy() {
		if (this._select) {
			this._select._optionMap.delete(this.id);
			this._select.writeValue(this._select.value);
		}
	}
	static ɵfac = function ɵNgSelectMultipleOption_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ɵNgSelectMultipleOption)(ɵɵdirectiveInject(ElementRef), ɵɵdirectiveInject(Renderer2), ɵɵdirectiveInject(SelectMultipleControlValueAccessor, 9));
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: ɵNgSelectMultipleOption,
		selectors: [["option"]],
		inputs: {
			ngValue: "ngValue",
			value: "value"
		},
		standalone: false
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ɵNgSelectMultipleOption, [{
		type: Directive,
		args: [{
			selector: "option",
			standalone: false
		}]
	}], () => [
		{ type: ElementRef },
		{ type: Renderer2 },
		{
			type: SelectMultipleControlValueAccessor,
			decorators: [{ type: Optional }, { type: Host }]
		}
	], {
		ngValue: [{
			type: Input,
			args: ["ngValue"]
		}],
		value: [{
			type: Input,
			args: ["value"]
		}]
	});
})();
var SHARED_FORM_DIRECTIVES = [
	ɵNgNoValidate,
	NgSelectOption,
	ɵNgSelectMultipleOption,
	DefaultValueAccessor,
	NumberValueAccessor,
	RangeValueAccessor,
	CheckboxControlValueAccessor,
	SelectControlValueAccessor,
	SelectMultipleControlValueAccessor,
	RadioControlValueAccessor,
	NgControlStatus,
	NgControlStatusGroup,
	RequiredValidator,
	MinLengthValidator,
	MaxLengthValidator,
	PatternValidator,
	CheckboxRequiredValidator,
	EmailValidator,
	MinValidator,
	MaxValidator
];
var TEMPLATE_DRIVEN_DIRECTIVES = [
	NgModel,
	NgModelGroup,
	NgForm
];
var REACTIVE_DRIVEN_DIRECTIVES = [
	FormControlDirective,
	FormGroupDirective,
	FormArrayDirective,
	FormControlName,
	FormGroupName,
	FormArrayName
];
var ɵInternalFormsSharedModule = class ɵInternalFormsSharedModule {
	static ɵfac = function ɵInternalFormsSharedModule_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ɵInternalFormsSharedModule)();
	};
	static ɵmod = /*@__PURE__*/ ɵɵdefineNgModule({
		type: ɵInternalFormsSharedModule,
		declarations: [
			ɵNgNoValidate,
			NgSelectOption,
			ɵNgSelectMultipleOption,
			DefaultValueAccessor,
			NumberValueAccessor,
			RangeValueAccessor,
			CheckboxControlValueAccessor,
			SelectControlValueAccessor,
			SelectMultipleControlValueAccessor,
			RadioControlValueAccessor,
			NgControlStatus,
			NgControlStatusGroup,
			RequiredValidator,
			MinLengthValidator,
			MaxLengthValidator,
			PatternValidator,
			CheckboxRequiredValidator,
			EmailValidator,
			MinValidator,
			MaxValidator
		],
		exports: [
			ɵNgNoValidate,
			NgSelectOption,
			ɵNgSelectMultipleOption,
			DefaultValueAccessor,
			NumberValueAccessor,
			RangeValueAccessor,
			CheckboxControlValueAccessor,
			SelectControlValueAccessor,
			SelectMultipleControlValueAccessor,
			RadioControlValueAccessor,
			NgControlStatus,
			NgControlStatusGroup,
			RequiredValidator,
			MinLengthValidator,
			MaxLengthValidator,
			PatternValidator,
			CheckboxRequiredValidator,
			EmailValidator,
			MinValidator,
			MaxValidator
		]
	});
	static ɵinj = /*@__PURE__*/ ɵɵdefineInjector({});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ɵInternalFormsSharedModule, [{
		type: NgModule,
		args: [{
			declarations: SHARED_FORM_DIRECTIVES,
			exports: SHARED_FORM_DIRECTIVES
		}]
	}], null, null);
})();
function isAbstractControlOptions(options) {
	return !!options && (options.asyncValidators !== void 0 || options.validators !== void 0 || options.updateOn !== void 0);
}
var FormBuilder = class FormBuilder {
	useNonNullable = false;
	get nonNullable() {
		const nnfb = new FormBuilder();
		nnfb.useNonNullable = true;
		return nnfb;
	}
	group(controls, options = null) {
		const reducedControls = this._reduceControls(controls);
		let newOptions = {};
		if (isAbstractControlOptions(options)) newOptions = options;
		else if (options !== null) {
			newOptions.validators = options.validator;
			newOptions.asyncValidators = options.asyncValidator;
		}
		return new FormGroup(reducedControls, newOptions);
	}
	record(controls, options = null) {
		return new FormRecord(this._reduceControls(controls), options);
	}
	control(formState, validatorOrOpts, asyncValidator) {
		let newOptions = {};
		if (!this.useNonNullable) return new FormControl(formState, validatorOrOpts, asyncValidator);
		if (isAbstractControlOptions(validatorOrOpts)) newOptions = validatorOrOpts;
		else {
			newOptions.validators = validatorOrOpts;
			newOptions.asyncValidators = asyncValidator;
		}
		return new FormControl(formState, {
			...newOptions,
			nonNullable: true
		});
	}
	array(controls, validatorOrOpts, asyncValidator) {
		return new FormArray(controls.map((c) => this._createControl(c)), validatorOrOpts, asyncValidator);
	}
	_reduceControls(controls) {
		const createdControls = {};
		Object.keys(controls).forEach((controlName) => {
			createdControls[controlName] = this._createControl(controls[controlName]);
		});
		return createdControls;
	}
	_createControl(controls) {
		if (controls instanceof FormControl) return controls;
		else if (controls instanceof AbstractControl) return controls;
		else if (Array.isArray(controls)) {
			const value = controls[0];
			const validator = controls.length > 1 ? controls[1] : null;
			const asyncValidator = controls.length > 2 ? controls[2] : null;
			return this.control(value, validator, asyncValidator);
		} else return this.control(controls);
	}
	static ɵfac = function FormBuilder_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormBuilder)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: FormBuilder,
		factory: FormBuilder.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormBuilder, [{ type: Service }], null, null);
})();
var NonNullableFormBuilder = class NonNullableFormBuilder {
	static ɵfac = function NonNullableFormBuilder_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || NonNullableFormBuilder)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: NonNullableFormBuilder,
		factory: () => (() => inject(FormBuilder).nonNullable)()
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NonNullableFormBuilder, [{
		type: Service,
		args: [{ factory: () => inject(FormBuilder).nonNullable }]
	}], null, null);
})();
var UntypedFormBuilder = class UntypedFormBuilder extends FormBuilder {
	group(controlsConfig, options = null) {
		return super.group(controlsConfig, options);
	}
	control(formState, validatorOrOpts, asyncValidator) {
		return super.control(formState, validatorOrOpts, asyncValidator);
	}
	array(controlsConfig, validatorOrOpts, asyncValidator) {
		return super.array(controlsConfig, validatorOrOpts, asyncValidator);
	}
	static ɵfac = function UntypedFormBuilder_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || UntypedFormBuilder)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: UntypedFormBuilder,
		factory: UntypedFormBuilder.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(UntypedFormBuilder, [{ type: Service }], null, null);
})();
var FormsModule = class FormsModule {
	static withConfig(opts) {
		return {
			ngModule: FormsModule,
			providers: [{
				provide: CALL_SET_DISABLED_STATE,
				useValue: opts.callSetDisabledState ?? setDisabledStateDefault
			}]
		};
	}
	static ɵfac = function FormsModule_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormsModule)();
	};
	static ɵmod = /*@__PURE__*/ ɵɵdefineNgModule({
		type: FormsModule,
		declarations: [
			NgModel,
			NgModelGroup,
			NgForm
		],
		exports: [
			ɵInternalFormsSharedModule,
			NgModel,
			NgModelGroup,
			NgForm
		]
	});
	static ɵinj = /*@__PURE__*/ ɵɵdefineInjector({ imports: [ɵInternalFormsSharedModule] });
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormsModule, [{
		type: NgModule,
		args: [{
			declarations: TEMPLATE_DRIVEN_DIRECTIVES,
			exports: [ɵInternalFormsSharedModule, TEMPLATE_DRIVEN_DIRECTIVES]
		}]
	}], null, null);
})();
var ReactiveFormsModule = class ReactiveFormsModule {
	static withConfig(opts) {
		return {
			ngModule: ReactiveFormsModule,
			providers: [{
				provide: NG_MODEL_WITH_FORM_CONTROL_WARNING,
				useValue: opts.warnOnNgModelWithFormControl ?? "always"
			}, {
				provide: CALL_SET_DISABLED_STATE,
				useValue: opts.callSetDisabledState ?? setDisabledStateDefault
			}]
		};
	}
	static ɵfac = function ReactiveFormsModule_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ReactiveFormsModule)();
	};
	static ɵmod = /*@__PURE__*/ ɵɵdefineNgModule({
		type: ReactiveFormsModule,
		declarations: [
			FormControlDirective,
			FormGroupDirective,
			FormArrayDirective,
			FormControlName,
			FormGroupName,
			FormArrayName
		],
		exports: [
			ɵInternalFormsSharedModule,
			FormControlDirective,
			FormGroupDirective,
			FormArrayDirective,
			FormControlName,
			FormGroupName,
			FormArrayName
		]
	});
	static ɵinj = /*@__PURE__*/ ɵɵdefineInjector({ imports: [ɵInternalFormsSharedModule] });
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ReactiveFormsModule, [{
		type: NgModule,
		args: [{
			declarations: [REACTIVE_DRIVEN_DIRECTIVES],
			exports: [ɵInternalFormsSharedModule, REACTIVE_DRIVEN_DIRECTIVES]
		}]
	}], null, null);
})();
// node_modules/@angular/forms/fesm2022/_validation_errors-chunk.mjs
/**
* @license Angular v22.2.4
* (c) 2010-2026 Google LLC. https://angular.dev/
* License: MIT
*/
var FIELD_TREE = /* @__PURE__ */ Symbol("FIELD_TREE");
function isArray(value) {
	return Array.isArray(value);
}
var MetadataReducer = {
	list() {
		return {
			reduce: (acc, item) => item === void 0 ? acc : [...acc, item],
			getInitial: () => []
		};
	},
	min() {
		return {
			reduce: (acc, item) => {
				if (acc === void 0 || item === void 0) return acc ?? item;
				return item < acc ? item : acc;
			},
			getInitial: () => void 0
		};
	},
	max() {
		return {
			reduce: (acc, item) => {
				if (acc === void 0 || item === void 0) return acc ?? item;
				return item > acc ? item : acc;
			},
			getInitial: () => void 0
		};
	},
	or() {
		return {
			reduce: (prev, next) => prev || next,
			getInitial: () => false
		};
	},
	and() {
		return {
			reduce: (prev, next) => prev && next,
			getInitial: () => true
		};
	},
	override
};
function override(getInitial) {
	return {
		reduce: (_, item) => item,
		getInitial: () => getInitial?.()
	};
}
var IS_ASYNC_VALIDATION_RESOURCE = Symbol("IS_ASYNC_VALIDATION_RESOURCE");
var MetadataKey = class {
	reducer;
	create;
	brand;
	[IS_ASYNC_VALIDATION_RESOURCE];
	constructor(reducer, create) {
		this.reducer = reducer;
		this.create = create;
	}
};
function createMetadataKey(reducer) {
	return new MetadataKey(reducer ?? MetadataReducer.override());
}
function createLimitSelectionKey() {
	return createMetadataKey();
}
createMetadataKey(MetadataReducer.or());
createLimitSelectionKey();
createMetadataKey(MetadataReducer.max());
createMetadataKey(MetadataReducer.max());
createLimitSelectionKey();
createMetadataKey(MetadataReducer.min());
createMetadataKey(MetadataReducer.min());
createMetadataKey(MetadataReducer.max());
createMetadataKey(MetadataReducer.min());
createMetadataKey(MetadataReducer.list());
function shallowArrayEquals(a, b) {
	if (a === b) return true;
	if (!a || !b) return false;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (!Object.is(a[i], b[i])) return false;
	return true;
}
function addDefaultField(errors, fieldTree) {
	if (isArray(errors)) for (const error of errors) error.fieldTree ??= fieldTree;
	else if (errors) errors.fieldTree ??= fieldTree;
	return errors;
}
createMetadataKey();
computed(() => false, ...ngDevMode ? [{ debugName: "FALSE_SIGNAL" }] : []);
computed(() => [], ...ngDevMode ? [{ debugName: "ROOT_PATH_KEYS" }] : []);
computed(() => {
	throw new RuntimeError(1905, ngDevMode && "The top-level field in the form has no parent.");
}, ...ngDevMode ? [{ debugName: "ROOT_KEY_IN_PARENT" }] : []);
computed(() => [], ...ngDevMode ? [{ debugName: "EMPTY" }] : []);
computed(() => false, ...ngDevMode ? [{ debugName: "FALSE" }] : []);
new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "REGISTER_WEBMCP_FORM" : "");
async function submit(form, options) {
	const node = untracked(form);
	if (untracked(node.submitState.submitting)) return false;
	const field = options === void 0 ? node.structure.root.fieldProxy : form;
	const detail = {
		root: node.structure.root.fieldProxy,
		submitted: form
	};
	options = typeof options === "function" ? { action: options } : options ?? node.structure.fieldManager.submitOptions;
	const action = options?.action;
	if (!action) throw new RuntimeError(1915, (typeof ngDevMode === "undefined" || ngDevMode) && "Cannot submit form with no submit action. Specify the action when creating the form, or as an additional argument to `submit()`.");
	node.markAsTouched();
	const onInvalid = options?.onInvalid;
	const shouldRun = shouldRunAction(node, options?.ignoreValidators);
	try {
		if (shouldRun) {
			node.submitState.selfSubmitting.set(true);
			const errors = await untracked(() => action?.(field, detail));
			errors && setSubmissionErrors(node, errors);
			return !errors || isArray(errors) && errors.length === 0;
		} else untracked(() => onInvalid?.(field, detail));
		return false;
	} finally {
		node.submitState.selfSubmitting.set(false);
	}
}
function shouldRunAction(node, ignoreValidators) {
	switch (ignoreValidators) {
		case "all": return true;
		case "none": return untracked(node.valid);
		default: return !untracked(node.invalid);
	}
}
function setSubmissionErrors(submittedField, errors) {
	if (!isArray(errors)) errors = [errors];
	const errorsByField = /* @__PURE__ */ new Map();
	for (const error of errors) {
		const errorWithField = addDefaultField(error, submittedField.fieldTree);
		const field = errorWithField.fieldTree();
		let fieldErrors = errorsByField.get(field);
		if (!fieldErrors) {
			fieldErrors = [];
			errorsByField.set(field, fieldErrors);
		}
		fieldErrors.push(errorWithField);
	}
	for (const [field, fieldErrors] of errorsByField) field.submitState.submissionErrors.set(fieldErrors);
}
var CompatValidationError = class {
	kind = "compat";
	control;
	fieldTree;
	context;
	message;
	constructor({ context, kind, control }) {
		this.context = context;
		this.kind = kind;
		this.control = control;
	}
};
function signalErrorsToValidationErrors(errors) {
	if (errors.length === 0) return null;
	const errObj = {};
	for (const error of errors) errObj[error.kind] = error instanceof CompatValidationError ? error.context : error;
	return errObj;
}
function reactiveErrorsToSignalErrors(errors, control) {
	if (errors === null) return [];
	return Object.entries(errors).map(([kind, context]) => {
		return new CompatValidationError({
			context,
			kind,
			control
		});
	});
}
// node_modules/@angular/forms/fesm2022/signals.mjs
/**
* @license Angular v22.2.4
* (c) 2010-2026 Google LLC. https://angular.dev/
* License: MIT
*/
function isFieldTree(value) {
	return typeof value === "function" && value[FIELD_TREE] === true;
}
var SIGNAL_FORMS_CONFIG = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "SIGNAL_FORMS_CONFIG" : "");
function normalizeErrors(error) {
	if (error === void 0) return [];
	if (Array.isArray(error)) return error;
	return [error];
}
var BaseNgValidationError = class {
	__brand = void 0;
	kind = "";
	fieldTree;
	message;
	constructor(options) {
		if (options) Object.assign(this, options);
	}
};
var NativeInputParseError = class extends BaseNgValidationError {
	kind = "parse";
};
function createParser(getValue, setValue, parse) {
	const errors = linkedSignal({
		...ngDevMode ? { debugName: "errors" } : {},
		source: getValue,
		computation: () => [],
		equal: shallowArrayEquals
	});
	const setRawValue = (rawValue) => {
		const result = parse(rawValue);
		errors.set(normalizeErrors(result.error));
		if (result.value !== void 0) setValue(result.value);
		errors.set(normalizeErrors(result.error));
	};
	const reset = () => {
		errors.set([]);
	};
	return {
		errors: errors.asReadonly(),
		setRawValue,
		reset
	};
}
var InteropNgControl = class {
	field;
	constructor(field) {
		this.field = field;
	}
	control = this;
	get value() {
		return this.field().controlValue();
	}
	get valid() {
		return this.field().valid();
	}
	get invalid() {
		return this.field().invalid();
	}
	get pending() {
		return this.field().pending();
	}
	get disabled() {
		return this.field().disabled();
	}
	get enabled() {
		return !this.field().disabled();
	}
	get errors() {
		return signalErrorsToValidationErrors(this.field().errors());
	}
	get pristine() {
		return !this.field().dirty();
	}
	get dirty() {
		return this.field().dirty();
	}
	get touched() {
		return this.field().touched();
	}
	get untouched() {
		return !this.field().touched();
	}
	get status() {
		if (this.field().disabled()) return "DISABLED";
		if (this.field().valid()) return "VALID";
		if (this.field().invalid()) return "INVALID";
		if (this.field().pending()) return "PENDING";
		throw new RuntimeError(1910, ngDevMode && "Unknown form control status");
	}
	valueAccessor = null;
	hasValidator(validator) {
		if (validator === Validators.required) return this.field().required();
		return false;
	}
	updateValueAndValidity() {}
};
var FIELD_STATE_KEY_TO_CONTROL_BINDING = {
	disabled: "disabled",
	disabledReasons: "disabledReasons",
	dirty: "dirty",
	errors: "errors",
	hidden: "hidden",
	invalid: "invalid",
	max: "max",
	maxLength: "maxLength",
	min: "min",
	minLength: "minLength",
	name: "name",
	pattern: "pattern",
	pending: "pending",
	readonly: "readonly",
	required: "required",
	touched: "touched"
};
var CONTROL_BINDING_TO_FIELD_STATE_KEY = /* @__PURE__ */ (() => {
	const map = {};
	for (const key of Object.keys(FIELD_STATE_KEY_TO_CONTROL_BINDING)) map[FIELD_STATE_KEY_TO_CONTROL_BINDING[key]] = key;
	return map;
})();
function readFieldStateBindingValue(fieldState, key) {
	return fieldState[CONTROL_BINDING_TO_FIELD_STATE_KEY[key]]?.();
}
var CONTROL_BINDING_NAMES = /* @__PURE__ */ (() => Object.values(FIELD_STATE_KEY_TO_CONTROL_BINDING))();
function createBindings() {
	return {};
}
function bindingUpdated(bindings, key, value) {
	if (!Object.is(bindings[key], value)) {
		bindings[key] = value;
		return true;
	}
	return false;
}
function getNativeControlValue(element, currentValue, validityMonitor) {
	let modelValue;
	if (isInput(element) && validityMonitor.isBadInput(element)) return { error: new NativeInputParseError() };
	switch (element.type) {
		case "checkbox": return { value: element.checked };
		case "number":
		case "range":
		case "datetime-local":
			modelValue = untracked(currentValue);
			if (typeof modelValue === "number" || modelValue === null) return { value: element.value === "" ? null : element.valueAsNumber };
			break;
		case "date":
		case "month":
		case "time":
		case "week":
			modelValue = untracked(currentValue);
			if (modelValue === null || modelValue instanceof Date) return { value: element.valueAsDate };
			else if (typeof modelValue === "number") return { value: element.valueAsNumber };
	}
	if (element.tagName === "INPUT" && element.type === "text") {
		modelValue ??= untracked(currentValue);
		if (typeof modelValue === "number" || modelValue === null) {
			if (element.value === "") return { value: null };
			const parsed = Number(element.value);
			if (Number.isNaN(parsed)) return { error: new NativeInputParseError() };
			return { value: parsed };
		}
	}
	return { value: element.value };
}
function setNativeControlValue(element, value) {
	switch (element.type) {
		case "checkbox":
			element.checked = value;
			return;
		case "radio":
			element.checked = value === element.value;
			return;
		case "number":
		case "range":
		case "datetime-local":
			if (typeof value === "number") {
				setNativeNumberControlValue(element, value);
				return;
			} else if (value === null) {
				element.value = "";
				return;
			}
			break;
		case "date":
		case "month":
		case "time":
		case "week": if (value === null || value instanceof Date) {
			element.valueAsDate = value;
			return;
		} else if (typeof value === "number") {
			setNativeNumberControlValue(element, value);
			return;
		}
	}
	if (element.tagName === "INPUT" && element.type === "text") {
		if (typeof value === "number") {
			element.value = isNaN(value) ? "" : String(value);
			return;
		}
		if (value === null) {
			if (typeof ngDevMode !== "undefined" && ngDevMode) console.warn(formatRuntimeError(1921, `The text input ${element.name} received a null value. Text inputs should use empty strings to represent null values.  The input's value will be set to an empty string instead.`));
			element.value = "";
			return;
		}
	}
	element.value = value;
}
function setNativeNumberControlValue(element, value) {
	if (isNaN(value)) element.value = "";
	else element.valueAsNumber = value;
}
function isInput(element) {
	return element.tagName === "INPUT";
}
function inputRequiresValidityTracking(input) {
	return input.type === "date" || input.type === "datetime-local" || input.type === "month" || input.type === "time" || input.type === "week";
}
function formatDateForInput(date, type) {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, "0");
	if (type === "month") return `${year}-${month}`;
	return `${year}-${month}-${String(date.getUTCDate()).padStart(2, "0")}`;
}
function formatDateForMinMax(name, value, type) {
	if (value instanceof Date && (name === "min" || name === "max") && (type === "date" || type === "month")) return formatDateForInput(value, type);
	return value;
}
function customControlCreate(host, parent) {
	host.listenToCustomControlModel((value) => parent.state().controlValue.set(value));
	host.listenToCustomControlOutput("touch", () => parent.state().markAsTouched());
	parent.registerAsBinding(host.customControl);
	const bindings = createBindings();
	return () => {
		const state = parent.state();
		const controlValue = state.controlValue();
		if (bindingUpdated(bindings, "controlValue", controlValue)) host.setCustomControlModelInput(controlValue);
		for (const name of CONTROL_BINDING_NAMES) {
			let value;
			if (name === "errors") value = parent.errors();
			else value = readFieldStateBindingValue(state, name);
			if (bindingUpdated(bindings, name, value)) {
				host.setInputOnDirectives(name, value);
				if (parent.elementAcceptsNativeProperty(name) && !host.customControlHasInput(name)) {
					const domValue = formatDateForMinMax(name, value, parent.nativeFormElement.type);
					setNativeDomProperty(parent.renderer, parent.nativeFormElement, name, domValue);
				}
			}
		}
	};
}
function isValidatorObject(v) {
	return typeof v === "object" && v !== null;
}
function cvaControlCreate(host, parent) {
	const bindings = createBindings();
	parent.controlValueAccessor.registerOnChange((value) => {
		bindings["controlValue"] = value;
		parent.state().controlValue.set(value);
	});
	parent.controlValueAccessor.registerOnTouched(() => parent.state().markAsTouched());
	const legacyValidators = parent.injector.get(NG_VALIDATORS, null, {
		optional: true,
		self: true
	});
	if (legacyValidators) {
		let version;
		for (const v of legacyValidators) if (isValidatorObject(v) && v.registerOnValidatorChange) {
			version ??= signal(0);
			v.registerOnValidatorChange(() => {
				version.update((n) => n + 1);
			});
		}
		const validatorFns = legacyValidators.map((v) => typeof v === "function" ? v : v.validate.bind(v));
		const mergedValidator = Validators.compose(validatorFns);
		const parseErrors = computed(() => {
			version?.();
			return reactiveErrorsToSignalErrors(mergedValidator ? mergedValidator(parent.interopNgControl.control) : null, parent.interopNgControl.control);
		}, ...ngDevMode ? [{ debugName: "parseErrors" }] : []);
		parent.parseErrorsSource.set(parseErrors);
	}
	parent.registerAsBinding({ reset: () => {
		const value = parent.state().value();
		bindings["controlValue"] = value;
		untracked(() => parent.controlValueAccessor.writeValue(value));
	} });
	return () => {
		const fieldState = parent.state();
		const controlValue = fieldState.controlValue();
		if (bindingUpdated(bindings, "controlValue", controlValue)) untracked(() => parent.controlValueAccessor.writeValue(controlValue));
		for (const name of CONTROL_BINDING_NAMES) {
			const value = readFieldStateBindingValue(fieldState, name);
			if (bindingUpdated(bindings, name, value)) {
				const propertyWasSet = host.setInputOnDirectives(name, value, name === "name" ? isDefinedPredicate : void 0);
				if (name === "disabled" && parent.controlValueAccessor.setDisabledState) untracked(() => parent.controlValueAccessor.setDisabledState(value));
				else if (!propertyWasSet && parent.elementAcceptsNativeProperty(name)) setNativeDomProperty(parent.renderer, parent.nativeFormElement, name, value);
			}
		}
	};
}
function isDefinedPredicate(value) {
	return value == null;
}
function observeSelectMutations(select, onMutation, destroyRef) {
	if (typeof MutationObserver !== "function") return;
	const observer = new MutationObserver((mutations) => {
		if (mutations.some((m) => isRelevantSelectMutation(m))) onMutation();
	});
	observer.observe(select, {
		attributes: true,
		attributeFilter: ["value"],
		characterData: true,
		childList: true,
		subtree: true
	});
	destroyRef.onDestroy(() => observer.disconnect());
}
function isRelevantSelectMutation(mutation) {
	if (mutation.type === "childList" || mutation.type === "characterData") {
		if (mutation.target instanceof Comment) return false;
		for (const node of mutation.addedNodes) if (!(node instanceof Comment)) return true;
		for (const node of mutation.removedNodes) if (!(node instanceof Comment)) return true;
		return false;
	}
	if (mutation.type === "attributes" && mutation.target instanceof HTMLOptionElement) return true;
	return false;
}
function nativeControlCreate(host, parent, parseErrorsSource, validityMonitor) {
	let updateMode = false;
	const input = parent.nativeFormElement;
	const parser = createParser(() => parent.state().value(), (rawValue) => parent.state().controlValue.set(rawValue), (_rawValue) => getNativeControlValue(input, parent.state().value, validityMonitor));
	parseErrorsSource.set(parser.errors);
	parent.onReset = () => {
		parser.reset();
		const value = parent.state().value();
		bindings["controlValue"] = value;
		setNativeControlValue(input, value);
	};
	const nativeEl = host.nativeElement;
	const onInput = () => parser.setRawValue(void 0);
	nativeEl.addEventListener("input", onInput);
	const onBlur = () => parent.state().markAsTouched();
	nativeEl.addEventListener("blur", onBlur);
	parent.destroyRef.onDestroy(() => {
		nativeEl.removeEventListener("input", onInput);
		nativeEl.removeEventListener("blur", onBlur);
	});
	if (isInput(input) && inputRequiresValidityTracking(input)) validityMonitor.watchValidity(parent.destroyRef, input, () => parser.setRawValue(void 0));
	parent.registerAsBinding();
	if (input.tagName === "SELECT") observeSelectMutations(input, () => {
		if (!updateMode) return;
		input.value = parent.state().controlValue();
	}, parent.destroyRef);
	const bindings = createBindings();
	return () => {
		const state = parent.state();
		for (const name of CONTROL_BINDING_NAMES) {
			const value = readFieldStateBindingValue(state, name);
			if (bindingUpdated(bindings, name, value)) {
				host.setInputOnDirectives(name, value);
				if (parent.elementAcceptsNativeProperty(name)) {
					const domValue = formatDateForMinMax(name, value, input.type);
					setNativeDomProperty(parent.renderer, input, name, domValue);
				}
			}
		}
		const controlValue = state.controlValue();
		const controlValueChanged = bindingUpdated(bindings, "controlValue", controlValue);
		const radioValueChanged = input.type === "radio" && bindingUpdated(bindings, "radioValue", input.value);
		if (controlValueChanged || radioValueChanged) setNativeControlValue(input, controlValue);
		updateMode = true;
	};
}
var InputValidityMonitor = class InputValidityMonitor {
	static ɵfac = function InputValidityMonitor_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || InputValidityMonitor)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineInjectable({
		token: InputValidityMonitor,
		factory: (__ngFactoryType__) => AnimationInputValidityMonitor.ɵfac(__ngFactoryType__),
		providedIn: "root"
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(InputValidityMonitor, [{
		type: Injectable,
		args: [{
			providedIn: "root",
			useClass: forwardRef(() => AnimationInputValidityMonitor)
		}]
	}], null, null);
})();
var AnimationInputValidityMonitor = class AnimationInputValidityMonitor extends InputValidityMonitor {
	document = inject(DOCUMENT);
	cspNonce = inject(CSP_NONCE, { optional: true });
	injectedStyles = /* @__PURE__ */ new WeakMap();
	watchValidity(destroyRef, element, callback) {
		const rootNode = element.getRootNode();
		if (!this.injectedStyles.has(rootNode)) this.injectedStyles.set(rootNode, this.createAnimationStyle(rootNode));
		const onAnimationStart = (event) => {
			const animationEvent = event;
			if (animationEvent.animationName === "ng-valid" || animationEvent.animationName === "ng-invalid") callback();
		};
		element.addEventListener("animationstart", onAnimationStart);
		destroyRef.onDestroy(() => {
			element.removeEventListener("animationstart", onAnimationStart);
		});
	}
	isBadInput(element) {
		return element.validity?.badInput ?? false;
	}
	createAnimationStyle(rootNode) {
		const element = this.document.createElement("style");
		if (this.cspNonce) element.nonce = this.cspNonce;
		element.textContent = `
      @keyframes ng-valid {}
      @keyframes ng-invalid {}
      input:valid, textarea:valid {
        animation: ng-valid 0.001s;
      }
      input:invalid, textarea:invalid {
        animation: ng-invalid 0.001s;
      }
    `;
		if (rootNode.nodeType === 9) rootNode.head?.appendChild(element);
		else rootNode.appendChild(element);
		return element;
	}
	ngOnDestroy() {
		this.injectedStyles.get(this.document)?.remove();
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵAnimationInputValidityMonitor_BaseFactory = void 0;
		return function AnimationInputValidityMonitor_Factory(__ngFactoryType__) {
			return (ɵAnimationInputValidityMonitor_BaseFactory || (ɵAnimationInputValidityMonitor_BaseFactory = ɵɵgetInheritedFactory(AnimationInputValidityMonitor)))(__ngFactoryType__ || AnimationInputValidityMonitor);
		};
	})();
	static ɵprov = /*@__PURE__*/ ɵɵdefineInjectable({
		token: AnimationInputValidityMonitor,
		factory: AnimationInputValidityMonitor.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(AnimationInputValidityMonitor, [{ type: Injectable }], null, null);
})();
var ɵNgFieldDirective = Symbol();
var FORM_FIELD = new InjectionToken(typeof ngDevMode !== "undefined" && ngDevMode ? "FORM_FIELD" : "");
var FormField = class FormField {
	field = input.required({
		...ngDevMode ? { debugName: "field" } : {},
		alias: "formField"
	});
	state = computed(() => this.field()(), ...ngDevMode ? [{ debugName: "state" }] : []);
	renderer = inject(Renderer2);
	destroyRef = inject(DestroyRef);
	injector = inject(Injector);
	element = inject(ElementRef).nativeElement;
	elementIsNativeFormElement = isNativeFormElement(this.element);
	elementAcceptsTextualValues = isTextualFormElement(this.element);
	_elementAcceptsMinMax;
	nativeFormElement = this.elementIsNativeFormElement ? this.element : void 0;
	focuser = (options) => this.element.focus(options);
	controlValueAccessors = inject(NG_VALUE_ACCESSOR, {
		optional: true,
		self: true
	});
	config = inject(SIGNAL_FORMS_CONFIG, { optional: true });
	validityMonitor = inject(InputValidityMonitor);
	parseErrorsSource = signal(void 0, ...ngDevMode ? [{ debugName: "parseErrorsSource" }] : []);
	_interopNgControl;
	get interopNgControl() {
		return this._interopNgControl ??= new InteropNgControl(this.state);
	}
	parseErrors = computed(() => this.parseErrorsSource()?.().map((err) => ({
		...err,
		fieldTree: untracked(this.state).fieldTree,
		formField: this
	})) ?? [], {
		...ngDevMode ? { debugName: "parseErrors" } : {},
		equal: shallowArrayEquals
	});
	errors = computed(() => this.state().errors().filter((err) => !err.formField || err.formField === this), {
		...ngDevMode ? { debugName: "errors" } : {},
		equal: shallowArrayEquals
	});
	isFieldBinding = false;
	resetter = () => {};
	parseErrorsResetCallback;
	setParseErrors(source) {
		this.parseErrorsSource.set(source);
	}
	set onReset(callback) {
		this.parseErrorsResetCallback = callback;
	}
	get onReset() {
		return this.parseErrorsResetCallback;
	}
	get controlValueAccessor() {
		if (!this.controlValueAccessors || this.controlValueAccessors.length === 0) return this.interopNgControl?.valueAccessor ?? void 0;
		return selectValueAccessor(this.interopNgControl, this.controlValueAccessors) ?? void 0;
	}
	installClassBindingEffect() {
		const classes = Object.entries(this.config?.classes ?? {}).map(([className, computation]) => [className, computed(() => computation(this))]);
		if (classes.length === 0) return;
		const bindings = createBindings();
		afterRenderEffect({ write: () => {
			if (this.state().structure.isOrphaned()) return;
			for (const [className, computation] of classes) {
				const active = computation();
				if (bindingUpdated(bindings, className, active)) {
					if (active) this.renderer.addClass(this.element, className);
					else this.renderer.removeClass(this.element, className);
				}
			}
		} }, { injector: this.injector });
	}
	focus(options) {
		this.focuser(options);
	}
	reset() {
		this.resetter();
		this.parseErrorsResetCallback?.(this.state().value());
	}
	registerAsBinding(bindingOptions) {
		if (this.isFieldBinding) throw new RuntimeError(1913, typeof ngDevMode !== "undefined" && ngDevMode && "FormField already registered as a binding");
		this.isFieldBinding = true;
		this.installClassBindingEffect();
		if (bindingOptions?.focus) this.focuser = (focusOptions) => bindingOptions.focus(focusOptions);
		if (bindingOptions?.reset) this.resetter = () => bindingOptions.reset();
		effect((onCleanup) => {
			const fieldNode = this.state();
			fieldNode.nodeState.formFieldBindings.update((controls) => [...controls, this]);
			onCleanup(() => {
				const remaining = fieldNode.nodeState.formFieldBindings().filter((c) => c !== this);
				fieldNode.nodeState.formFieldBindings.set(remaining);
				if (remaining.length === 0 && !fieldNode.structure.isOrphaned()) fieldNode.flushSync();
			});
		}, { injector: this.injector });
		if (typeof ngDevMode !== "undefined" && ngDevMode) effect(() => {
			const fieldNode = this.state();
			if (fieldNode.hidden()) {
				const path = fieldNode.structure.pathKeys().join(".") || "<root>";
				console.warn(formatRuntimeError(1916, `Field '${path}' is hidden but is being rendered. Hidden fields should be removed from the DOM using @if.`));
			}
		}, { injector: this.injector });
	}
	[ɵNgFieldDirective];
	ɵngControlCreate(host) {
		if (host.hasPassThrough) return;
		if (this.controlValueAccessor) this.ɵngControlUpdate = cvaControlCreate(host, this);
		else if (host.customControl) this.ɵngControlUpdate = customControlCreate(host, this);
		else if (this.elementIsNativeFormElement) this.ɵngControlUpdate = nativeControlCreate(host, this, this.parseErrorsSource, this.validityMonitor);
		else throw new RuntimeError(1914, typeof ngDevMode !== "undefined" && ngDevMode && `${host.descriptor} is an invalid [formField] directive host. The host must be a native form control (such as <input>', '<select>', or '<textarea>') or a custom form control with a 'value' or 'checked' model.`);
	}
	ɵngControlUpdate;
	elementAcceptsNativeProperty(key) {
		if (!this.elementIsNativeFormElement) return false;
		switch (key) {
			case "min":
			case "max": return this._elementAcceptsMinMax ??= elementAcceptsMinMax(this.element);
			case "minLength":
			case "maxLength": return this.elementAcceptsTextualValues;
			case "disabled":
			case "required":
			case "readonly":
			case "name": return true;
			default: return false;
		}
	}
	static ɵfac = function FormField_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormField)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormField,
		selectors: [[
			"",
			"formField",
			""
		]],
		inputs: { field: [
			1,
			"formField",
			"field"
		] },
		exportAs: ["formField"],
		features: [ɵɵProvidersFeature([
			{
				provide: FORM_FIELD,
				useExisting: FormField
			},
			{
				provide: NgControl,
				useFactory: () => inject(FormField).interopNgControl
			},
			{
				provide: ɵFORM_CONTROL_INTEGRATION,
				useFactory: () => inject(FORM_FIELD, { self: true })
			}
		]), ɵɵControlFeature("formField")]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormField, [{
		type: Directive,
		args: [{
			selector: "[formField]",
			exportAs: "formField",
			providers: [
				{
					provide: FORM_FIELD,
					useExisting: FormField
				},
				{
					provide: NgControl,
					useFactory: () => inject(FormField).interopNgControl
				},
				{
					provide: ɵFORM_CONTROL_INTEGRATION,
					useFactory: () => inject(FORM_FIELD, { self: true })
				}
			]
		}]
	}], null, { field: [{
		type: Input,
		args: [{
			isSignal: true,
			alias: "formField",
			required: true
		}]
	}] });
})();
var FormRoot = class FormRoot {
	fieldTree = input.required({
		...ngDevMode ? { debugName: "fieldTree" } : {},
		alias: "formRoot"
	});
	onSubmit(event) {
		event.preventDefault();
		untracked(() => {
			const fieldTree = this.fieldTree();
			if (fieldTree().structure.fieldManager.submitOptions) submit(fieldTree);
		});
	}
	static ɵfac = function FormRoot_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || FormRoot)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: FormRoot,
		selectors: [[
			"form",
			"formRoot",
			""
		]],
		hostAttrs: ["novalidate", ""],
		hostBindings: function FormRoot_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("submit", function FormRoot_submit_HostBindingHandler($event) {
				return ctx.onSubmit($event);
			});
		},
		inputs: { fieldTree: [
			1,
			"formRoot",
			"fieldTree"
		] }
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(FormRoot, [{
		type: Directive,
		args: [{
			selector: "form[formRoot]",
			host: {
				"novalidate": "",
				"(submit)": "onSubmit($event)"
			}
		}]
	}], null, { fieldTree: [{
		type: Input,
		args: [{
			isSignal: true,
			alias: "formRoot",
			required: true
		}]
	}] });
})();
// node_modules/@angular/material/fesm2022/_error-options-chunk.mjs
var ShowOnDirtyErrorStateMatcher = class ShowOnDirtyErrorStateMatcher {
	isErrorState(control, form) {
		return !!(control && control.invalid && (control.dirty || form && form.submitted));
	}
	isSignalErrorState(field) {
		if (!field) return false;
		const invalid = field().invalid();
		const dirty = field().dirty();
		return invalid && dirty;
	}
	static ɵfac = function ShowOnDirtyErrorStateMatcher_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ShowOnDirtyErrorStateMatcher)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: ShowOnDirtyErrorStateMatcher,
		factory: ShowOnDirtyErrorStateMatcher.ɵfac,
		autoProvided: false
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ShowOnDirtyErrorStateMatcher, [{
		type: Service,
		args: [{ autoProvided: false }]
	}], null, null);
})();
var ErrorStateMatcher = class ErrorStateMatcher {
	isErrorState(control, form) {
		return !!(control && control.invalid && (control.touched || form && form.submitted));
	}
	isSignalErrorState(field) {
		if (!field) return false;
		const invalid = field().invalid();
		const touched = field().touched();
		return invalid && touched;
	}
	static ɵfac = function ErrorStateMatcher_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || ErrorStateMatcher)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: ErrorStateMatcher,
		factory: ErrorStateMatcher.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ErrorStateMatcher, [{ type: Service }], null, null);
})();
// node_modules/@angular/material/fesm2022/_error-state-chunk.mjs
var _ErrorStateTracker = class {
	_defaultMatcher;
	_parentFormGroup;
	_parentForm;
	_stateChanges;
	errorState = false;
	matcher;
	ngControl;
	formField;
	constructor(_defaultMatcher, directive, _parentFormGroup, _parentForm, _stateChanges) {
		this._defaultMatcher = _defaultMatcher;
		this._parentFormGroup = _parentFormGroup;
		this._parentForm = _parentForm;
		this._stateChanges = _stateChanges;
		if (!directive) this.ngControl = this.formField = null;
		else if (isSignal(directive.field) && !directive.updateValueAndValidity) {
			this.formField = directive;
			this.ngControl = null;
		} else {
			this.formField = null;
			this.ngControl = directive;
		}
	}
	updateErrorState() {
		const oldState = this.errorState;
		const newState = this._getCurrentErrorState(this.matcher || this._defaultMatcher);
		if (newState !== oldState) {
			this.errorState = newState;
			this._stateChanges.next();
		}
	}
	_getCurrentErrorState(matcher) {
		if (this.formField && matcher?.isSignalErrorState) return matcher.isSignalErrorState(this.formField.field()) ?? false;
		const parent = this._parentFormGroup || this._parentForm;
		const control = this.ngControl ? this.ngControl.control : null;
		return matcher?.isErrorState(control, parent) ?? false;
	}
};
// node_modules/@angular/cdk/fesm2022/observers-private.mjs
var loopLimitExceededErrorHandler = (e) => {
	if (e instanceof ErrorEvent && e.message === "ResizeObserver loop limit exceeded") console.error(`${e.message}. This could indicate a performance issue with your app. See https://github.com/WICG/resize-observer/blob/master/explainer.md#error-handling`);
};
var SingleBoxSharedResizeObserver = class {
	_box;
	_destroyed = new Subject();
	_resizeSubject = new Subject();
	_resizeObserver;
	_elementObservables = /* @__PURE__ */ new Map();
	constructor(_box) {
		this._box = _box;
		if (typeof ResizeObserver !== "undefined") this._resizeObserver = new ResizeObserver((entries) => this._resizeSubject.next(entries));
	}
	observe(target) {
		if (!this._elementObservables.has(target)) this._elementObservables.set(target, new Observable((observer) => {
			const subscription = this._resizeSubject.subscribe(observer);
			this._resizeObserver?.observe(target, { box: this._box });
			return () => {
				this._resizeObserver?.unobserve(target);
				subscription.unsubscribe();
				this._elementObservables.delete(target);
			};
		}).pipe(map((entries) => entries.filter((entry) => entry.target === target)), filter((entries) => entries.length > 0), shareReplay({
			bufferSize: 1,
			refCount: true
		}), takeUntil(this._destroyed)));
		return this._elementObservables.get(target);
	}
	destroy() {
		this._destroyed.next();
		this._destroyed.complete();
		this._resizeSubject.complete();
		this._elementObservables.clear();
	}
};
var SharedResizeObserver = class SharedResizeObserver {
	_cleanupErrorListener;
	_observers = /* @__PURE__ */ new Map();
	_ngZone = inject(NgZone);
	constructor() {
		if (typeof ResizeObserver !== "undefined" && (typeof ngDevMode === "undefined" || ngDevMode)) this._ngZone.runOutsideAngular(() => {
			const renderer = inject(RendererFactory2).createRenderer(null, null);
			this._cleanupErrorListener = renderer.listen("window", "error", loopLimitExceededErrorHandler);
		});
	}
	ngOnDestroy() {
		for (const [, observer] of this._observers) observer.destroy();
		this._observers.clear();
		this._cleanupErrorListener?.();
	}
	observe(target, options) {
		const box = options?.box || "content-box";
		if (!this._observers.has(box)) this._observers.set(box, new SingleBoxSharedResizeObserver(box));
		return this._observers.get(box).observe(target);
	}
	static ɵfac = function SharedResizeObserver_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || SharedResizeObserver)();
	};
	static ɵprov = /*@__PURE__*/ ɵɵdefineService({
		token: SharedResizeObserver,
		factory: SharedResizeObserver.ɵfac
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(SharedResizeObserver, [{ type: Service }], () => [], null);
})();
// node_modules/@angular/material/fesm2022/_form-field-chunk.mjs
var MatLabel = class MatLabel {
	static ɵfac = function MatLabel_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatLabel)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatLabel,
		selectors: [["mat-label"]]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatLabel, [{
		type: Directive,
		args: [{ selector: "mat-label" }]
	}], null, null);
})();
var MAT_ERROR = new InjectionToken("MatError");
var MatError = class MatError {
	id = inject(_IdGenerator).getId("mat-mdc-error-");
	static ɵfac = function MatError_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatError)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatError,
		selectors: [["mat-error"], [
			"",
			"matError",
			""
		]],
		hostAttrs: [
			1,
			"mat-mdc-form-field-error",
			"mat-mdc-form-field-bottom-align"
		],
		hostVars: 1,
		hostBindings: function MatError_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵdomProperty("id", ctx.id);
		},
		inputs: { id: "id" },
		features: [ɵɵProvidersFeature([{
			provide: MAT_ERROR,
			useExisting: MatError
		}])]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatError, [{
		type: Directive,
		args: [{
			selector: "mat-error, [matError]",
			host: {
				"class": "mat-mdc-form-field-error mat-mdc-form-field-bottom-align",
				"[id]": "id"
			},
			providers: [{
				provide: MAT_ERROR,
				useExisting: MatError
			}]
		}]
	}], null, { id: [{ type: Input }] });
})();
var MatHint = class MatHint {
	align = "start";
	id = inject(_IdGenerator).getId("mat-mdc-hint-");
	static ɵfac = function MatHint_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatHint)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatHint,
		selectors: [["mat-hint"]],
		hostAttrs: [
			1,
			"mat-mdc-form-field-hint",
			"mat-mdc-form-field-bottom-align"
		],
		hostVars: 4,
		hostBindings: function MatHint_HostBindings(rf, ctx) {
			if (rf & 2) {
				ɵɵdomProperty("id", ctx.id);
				ɵɵattribute("align", null);
				ɵɵclassProp("mat-mdc-form-field-hint-end", ctx.align === "end");
			}
		},
		inputs: {
			align: "align",
			id: "id"
		}
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatHint, [{
		type: Directive,
		args: [{
			selector: "mat-hint",
			host: {
				"class": "mat-mdc-form-field-hint mat-mdc-form-field-bottom-align",
				"[class.mat-mdc-form-field-hint-end]": "align === \"end\"",
				"[id]": "id",
				"[attr.align]": "null"
			}
		}]
	}], null, {
		align: [{ type: Input }],
		id: [{ type: Input }]
	});
})();
var MAT_PREFIX = new InjectionToken("MatPrefix");
var MatPrefix = class MatPrefix {
	set _isTextSelector(value) {
		this._isText = true;
	}
	_isText = false;
	static ɵfac = function MatPrefix_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatPrefix)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatPrefix,
		selectors: [
			[
				"",
				"matPrefix",
				""
			],
			[
				"",
				"matIconPrefix",
				""
			],
			[
				"",
				"matTextPrefix",
				""
			]
		],
		inputs: { _isTextSelector: [
			0,
			"matTextPrefix",
			"_isTextSelector"
		] },
		features: [ɵɵProvidersFeature([{
			provide: MAT_PREFIX,
			useExisting: MatPrefix
		}])]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatPrefix, [{
		type: Directive,
		args: [{
			selector: "[matPrefix], [matIconPrefix], [matTextPrefix]",
			providers: [{
				provide: MAT_PREFIX,
				useExisting: MatPrefix
			}]
		}]
	}], null, { _isTextSelector: [{
		type: Input,
		args: ["matTextPrefix"]
	}] });
})();
var MAT_SUFFIX = new InjectionToken("MatSuffix");
var MatSuffix = class MatSuffix {
	set _isTextSelector(value) {
		this._isText = true;
	}
	_isText = false;
	static ɵfac = function MatSuffix_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatSuffix)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatSuffix,
		selectors: [
			[
				"",
				"matSuffix",
				""
			],
			[
				"",
				"matIconSuffix",
				""
			],
			[
				"",
				"matTextSuffix",
				""
			]
		],
		inputs: { _isTextSelector: [
			0,
			"matTextSuffix",
			"_isTextSelector"
		] },
		features: [ɵɵProvidersFeature([{
			provide: MAT_SUFFIX,
			useExisting: MatSuffix
		}])]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatSuffix, [{
		type: Directive,
		args: [{
			selector: "[matSuffix], [matIconSuffix], [matTextSuffix]",
			providers: [{
				provide: MAT_SUFFIX,
				useExisting: MatSuffix
			}]
		}]
	}], null, { _isTextSelector: [{
		type: Input,
		args: ["matTextSuffix"]
	}] });
})();
var FLOATING_LABEL_PARENT = new InjectionToken("FloatingLabelParent");
var MatFormFieldFloatingLabel = class MatFormFieldFloatingLabel {
	_elementRef = inject(ElementRef);
	get floating() {
		return this._floating;
	}
	set floating(value) {
		this._floating = value;
		if (this.monitorResize) this._handleResize();
	}
	_floating = false;
	get monitorResize() {
		return this._monitorResize;
	}
	set monitorResize(value) {
		this._monitorResize = value;
		if (this._monitorResize) this._subscribeToResize();
		else this._resizeSubscription.unsubscribe();
	}
	_monitorResize = false;
	_resizeObserver = inject(SharedResizeObserver);
	_ngZone = inject(NgZone);
	_parent = inject(FLOATING_LABEL_PARENT);
	_resizeSubscription = new Subscription();
	ngOnDestroy() {
		this._resizeSubscription.unsubscribe();
	}
	getWidth() {
		return estimateScrollWidth(this._elementRef.nativeElement);
	}
	get element() {
		return this._elementRef.nativeElement;
	}
	_handleResize() {
		setTimeout(() => this._parent._handleLabelResized());
	}
	_subscribeToResize() {
		this._resizeSubscription.unsubscribe();
		this._ngZone.runOutsideAngular(() => {
			this._resizeSubscription = this._resizeObserver.observe(this._elementRef.nativeElement, { box: "border-box" }).subscribe(() => this._handleResize());
		});
	}
	static ɵfac = function MatFormFieldFloatingLabel_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatFormFieldFloatingLabel)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatFormFieldFloatingLabel,
		selectors: [[
			"label",
			"matFormFieldFloatingLabel",
			""
		]],
		hostAttrs: [
			1,
			"mdc-floating-label",
			"mat-mdc-floating-label"
		],
		hostVars: 2,
		hostBindings: function MatFormFieldFloatingLabel_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵclassProp("mdc-floating-label--float-above", ctx.floating);
		},
		inputs: {
			floating: "floating",
			monitorResize: "monitorResize"
		}
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatFormFieldFloatingLabel, [{
		type: Directive,
		args: [{
			selector: "label[matFormFieldFloatingLabel]",
			host: {
				"class": "mdc-floating-label mat-mdc-floating-label",
				"[class.mdc-floating-label--float-above]": "floating"
			}
		}]
	}], null, {
		floating: [{ type: Input }],
		monitorResize: [{ type: Input }]
	});
})();
function estimateScrollWidth(element) {
	const htmlEl = element;
	if (htmlEl.offsetParent !== null) return htmlEl.scrollWidth;
	const clone = htmlEl.cloneNode(true);
	clone.style.setProperty("position", "absolute");
	clone.style.setProperty("transform", "translate(-9999px, -9999px)");
	document.documentElement.appendChild(clone);
	const scrollWidth = clone.scrollWidth;
	clone.remove();
	return scrollWidth;
}
var ACTIVATE_CLASS = "mdc-line-ripple--active";
var DEACTIVATING_CLASS = "mdc-line-ripple--deactivating";
var MatFormFieldLineRipple = class MatFormFieldLineRipple {
	_elementRef = inject(ElementRef);
	_cleanupTransitionEnd;
	constructor() {
		const ngZone = inject(NgZone);
		const renderer = inject(Renderer2);
		ngZone.runOutsideAngular(() => {
			this._cleanupTransitionEnd = renderer.listen(this._elementRef.nativeElement, "transitionend", this._handleTransitionEnd);
		});
	}
	activate() {
		const classList = this._elementRef.nativeElement.classList;
		classList.remove(DEACTIVATING_CLASS);
		classList.add(ACTIVATE_CLASS);
	}
	deactivate() {
		this._elementRef.nativeElement.classList.add(DEACTIVATING_CLASS);
	}
	_handleTransitionEnd = (event) => {
		const classList = this._elementRef.nativeElement.classList;
		const isDeactivating = classList.contains(DEACTIVATING_CLASS);
		if (event.propertyName === "opacity" && isDeactivating) classList.remove(ACTIVATE_CLASS, DEACTIVATING_CLASS);
	};
	ngOnDestroy() {
		this._cleanupTransitionEnd();
	}
	static ɵfac = function MatFormFieldLineRipple_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatFormFieldLineRipple)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatFormFieldLineRipple,
		selectors: [[
			"div",
			"matFormFieldLineRipple",
			""
		]],
		hostAttrs: [1, "mdc-line-ripple"]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatFormFieldLineRipple, [{
		type: Directive,
		args: [{
			selector: "div[matFormFieldLineRipple]",
			host: { "class": "mdc-line-ripple" }
		}]
	}], () => [], null);
})();
var MatFormFieldNotchedOutline = class MatFormFieldNotchedOutline {
	_elementRef = inject(ElementRef);
	_ngZone = inject(NgZone);
	open = false;
	_notch;
	ngAfterViewInit() {
		const element = this._elementRef.nativeElement;
		const label = element.querySelector(".mdc-floating-label");
		if (label) {
			element.classList.add("mdc-notched-outline--upgraded");
			if (typeof requestAnimationFrame === "function") {
				label.style.transitionDuration = "0s";
				this._ngZone.runOutsideAngular(() => {
					requestAnimationFrame(() => label.style.transitionDuration = "");
				});
			}
		} else element.classList.add("mdc-notched-outline--no-label");
	}
	_setNotchWidth(labelWidth) {
		const notch = this._notch.nativeElement;
		if (!this.open || !labelWidth) notch.style.width = "";
		else notch.style.width = `calc(${labelWidth}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`;
	}
	_setMaxWidth(prefixAndSuffixWidth) {
		this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width", `calc(100% - ${prefixAndSuffixWidth}px)`);
	}
	static ɵfac = function MatFormFieldNotchedOutline_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatFormFieldNotchedOutline)();
	};
	static ɵcmp = (function() {
		const _c0 = ["notch"];
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatFormFieldNotchedOutline,
			selectors: [[
				"div",
				"matFormFieldNotchedOutline",
				""
			]],
			viewQuery: function MatFormFieldNotchedOutline_Query(rf, ctx) {
				if (rf & 1) ɵɵviewQuery(_c0, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._notch = _t.first);
				}
			},
			hostAttrs: [1, "mdc-notched-outline"],
			hostVars: 2,
			hostBindings: function MatFormFieldNotchedOutline_HostBindings(rf, ctx) {
				if (rf & 2) ɵɵclassProp("mdc-notched-outline--notched", ctx.open);
			},
			inputs: { open: [
				0,
				"matFormFieldNotchedOutlineOpen",
				"open"
			] },
			ngContentSelectors: ["*"],
			decls: 5,
			vars: 0,
			consts: [
				["notch", ""],
				[
					1,
					"mat-mdc-notch-piece",
					"mdc-notched-outline__leading"
				],
				[
					1,
					"mat-mdc-notch-piece",
					"mdc-notched-outline__notch"
				],
				[
					1,
					"mat-mdc-notch-piece",
					"mdc-notched-outline__trailing"
				]
			],
			template: function MatFormFieldNotchedOutline_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef();
					ɵɵdomElement(0, "div", 1);
					ɵɵdomElementStart(1, "div", 2, 0);
					ɵɵprojection(3);
					ɵɵdomElementEnd();
					ɵɵdomElement(4, "div", 3);
				}
			},
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatFormFieldNotchedOutline, [{
		type: Component,
		args: [{
			selector: "div[matFormFieldNotchedOutline]",
			host: {
				"class": "mdc-notched-outline",
				"[class.mdc-notched-outline--notched]": "open"
			},
			encapsulation: ViewEncapsulation.None,
			template: "<div class=\"mat-mdc-notch-piece mdc-notched-outline__leading\"></div>\n<div class=\"mat-mdc-notch-piece mdc-notched-outline__notch\" #notch>\n  <ng-content></ng-content>\n</div>\n<div class=\"mat-mdc-notch-piece mdc-notched-outline__trailing\"></div>\n"
		}]
	}], null, {
		open: [{
			type: Input,
			args: ["matFormFieldNotchedOutlineOpen"]
		}],
		_notch: [{
			type: ViewChild,
			args: ["notch"]
		}]
	});
})();
var MatFormFieldControl = class MatFormFieldControl {
	id;
	ngField = null;
	ngControl = null;
	focused = false;
	empty = false;
	shouldLabelFloat = false;
	required = false;
	disabled = false;
	errorState = false;
	controlType;
	autofilled;
	userAriaDescribedBy;
	disableAutomaticLabeling;
	describedByIds;
	stateChanges = null;
	value;
	static ɵfac = function MatFormFieldControl_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatFormFieldControl)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({ type: MatFormFieldControl });
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatFormFieldControl, [{ type: Directive }], null, null);
})();
function getMatFormFieldDuplicatedHintError(align) {
	return Error(`A hint was already declared for 'align="${align}"'.`);
}
function getMatFormFieldMissingControlError() {
	return Error("mat-form-field must contain a MatFormFieldControl.");
}
var MAT_FORM_FIELD = new InjectionToken("MatFormField");
var MAT_FORM_FIELD_DEFAULT_OPTIONS = new InjectionToken("MAT_FORM_FIELD_DEFAULT_OPTIONS");
var DEFAULT_APPEARANCE = "fill";
var DEFAULT_FLOAT_LABEL = "auto";
var DEFAULT_SUBSCRIPT_SIZING = "fixed";
var FLOATING_LABEL_DEFAULT_DOCKED_TRANSFORM = `translateY(-50%)`;
var MatFormField = class MatFormField {
	_elementRef = inject(ElementRef);
	_changeDetectorRef = inject(ChangeDetectorRef);
	_platform = inject(Platform);
	_idGenerator = inject(_IdGenerator);
	_ngZone = inject(NgZone);
	_defaults = inject(MAT_FORM_FIELD_DEFAULT_OPTIONS, { optional: true });
	_currentDirection;
	_unwrapMaybeSignal(value) {
		return isSignal(value) ? value() : value;
	}
	_textField;
	_iconPrefixContainer;
	_textPrefixContainer;
	_iconSuffixContainer;
	_textSuffixContainer;
	_floatingLabel;
	_notchedOutline;
	_lineRipple;
	_iconPrefixContainerSignal = viewChild("iconPrefixContainer", ...ngDevMode ? [{ debugName: "_iconPrefixContainerSignal" }] : []);
	_textPrefixContainerSignal = viewChild("textPrefixContainer", ...ngDevMode ? [{ debugName: "_textPrefixContainerSignal" }] : []);
	_iconSuffixContainerSignal = viewChild("iconSuffixContainer", ...ngDevMode ? [{ debugName: "_iconSuffixContainerSignal" }] : []);
	_textSuffixContainerSignal = viewChild("textSuffixContainer", ...ngDevMode ? [{ debugName: "_textSuffixContainerSignal" }] : []);
	_prefixSuffixContainers = computed(() => {
		return [
			this._iconPrefixContainerSignal(),
			this._textPrefixContainerSignal(),
			this._iconSuffixContainerSignal(),
			this._textSuffixContainerSignal()
		].map((container) => container?.nativeElement).filter((e) => e !== void 0);
	}, ...ngDevMode ? [{ debugName: "_prefixSuffixContainers" }] : []);
	_formFieldControl;
	_prefixChildren;
	_suffixChildren;
	_errorChildren;
	_hintChildren;
	_labelChild = contentChild(MatLabel, ...ngDevMode ? [{ debugName: "_labelChild" }] : []);
	get hideRequiredMarker() {
		return this._hideRequiredMarker;
	}
	set hideRequiredMarker(value) {
		this._hideRequiredMarker = coerceBooleanProperty(value);
	}
	_hideRequiredMarker = false;
	color = "primary";
	get floatLabel() {
		return this._floatLabel || this._defaults?.floatLabel || DEFAULT_FLOAT_LABEL;
	}
	set floatLabel(value) {
		if (value !== this._floatLabel) {
			this._floatLabel = value;
			this._changeDetectorRef.markForCheck();
		}
	}
	_floatLabel;
	get appearance() {
		return this._appearanceSignal();
	}
	set appearance(value) {
		const newAppearance = value || this._defaults?.appearance || DEFAULT_APPEARANCE;
		if (typeof ngDevMode === "undefined" || ngDevMode) {
			if (newAppearance !== "fill" && newAppearance !== "outline") throw new Error(`MatFormField: Invalid appearance "${newAppearance}", valid values are "fill" or "outline".`);
		}
		this._appearanceSignal.set(newAppearance);
	}
	_appearanceSignal = signal(DEFAULT_APPEARANCE, ...ngDevMode ? [{ debugName: "_appearanceSignal" }] : []);
	get subscriptSizing() {
		return this._subscriptSizing || this._defaults?.subscriptSizing || DEFAULT_SUBSCRIPT_SIZING;
	}
	set subscriptSizing(value) {
		this._subscriptSizing = value || this._defaults?.subscriptSizing || DEFAULT_SUBSCRIPT_SIZING;
	}
	_subscriptSizing = null;
	get hintLabel() {
		return this._hintLabel;
	}
	set hintLabel(value) {
		this._hintLabel = value;
		this._processHints();
	}
	_hintLabel = "";
	_hasIconPrefix = false;
	_hasTextPrefix = false;
	_hasIconSuffix = false;
	_hasTextSuffix = false;
	_labelId = this._idGenerator.getId("mat-mdc-form-field-label-");
	_hintLabelId = this._idGenerator.getId("mat-mdc-hint-");
	_describedByIds;
	get _control() {
		return this._explicitFormFieldControl || this._formFieldControl;
	}
	set _control(value) {
		this._explicitFormFieldControl = value;
	}
	_destroyed = new Subject();
	_isFocused = null;
	_explicitFormFieldControl;
	_previousControl = null;
	_previousControlValidatorFn = null;
	_stateChanges;
	_valueChanges;
	_describedByChanges;
	_outlineLabelOffsetResizeObserver = null;
	_animationsDisabled = _animationsDisabled();
	constructor() {
		const defaults = this._defaults;
		const dir = inject(Directionality);
		if (defaults) {
			if (defaults.appearance) this.appearance = defaults.appearance;
			this._hideRequiredMarker = Boolean(defaults?.hideRequiredMarker);
			if (defaults.color) this.color = defaults.color;
		}
		effect(() => this._currentDirection = dir.valueSignal());
		this._syncOutlineLabelOffset();
	}
	ngAfterViewInit() {
		this._updateFocusState();
		if (!this._animationsDisabled) this._ngZone.runOutsideAngular(() => {
			setTimeout(() => {
				this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled");
			}, 300);
		});
		this._changeDetectorRef.detectChanges();
	}
	ngAfterContentInit() {
		this._assertFormFieldControl();
		this._initializeSubscript();
		this._initializePrefixAndSuffix();
	}
	ngAfterContentChecked() {
		this._assertFormFieldControl();
		if (this._control !== this._previousControl) {
			this._initializeControl(this._previousControl);
			if (this._control.ngControl && this._control.ngControl.control) this._previousControlValidatorFn = isFieldTree(this._control.ngField) ? null : this._control.ngControl.control.validator;
			this._previousControl = this._control;
			this._changeDetectorRef.markForCheck();
		}
		if (this._control.ngControl && this._control.ngControl.control && !isFieldTree(this._control.ngField)) {
			if (this._control.ngControl.control.validator !== this._previousControlValidatorFn) this._changeDetectorRef.markForCheck();
		}
	}
	ngOnDestroy() {
		this._outlineLabelOffsetResizeObserver?.disconnect();
		this._stateChanges?.unsubscribe();
		this._valueChanges?.unsubscribe();
		this._describedByChanges?.unsubscribe();
		this._destroyed.next();
		this._destroyed.complete();
	}
	getLabelId = computed(() => this._hasFloatingLabel() ? this._labelId : null, ...ngDevMode ? [{ debugName: "getLabelId" }] : []);
	getConnectedOverlayOrigin() {
		return this._textField || this._elementRef;
	}
	_animateAndLockLabel() {
		if (this._hasFloatingLabel()) this.floatLabel = "always";
	}
	_initializeControl(previousControl) {
		const control = this._control;
		const classPrefix = "mat-mdc-form-field-type-";
		if (previousControl) this._elementRef.nativeElement.classList.remove(classPrefix + previousControl.controlType);
		if (control.controlType) this._elementRef.nativeElement.classList.add(classPrefix + control.controlType);
		this._stateChanges?.unsubscribe();
		this._stateChanges = control.stateChanges?.subscribe(() => {
			this._updateFocusState();
			this._changeDetectorRef.markForCheck();
		});
		this._describedByChanges?.unsubscribe();
		this._describedByChanges = control.stateChanges?.pipe(startWith([void 0, void 0]), map(() => [this._unwrapMaybeSignal(control.errorState), control.userAriaDescribedBy]), pairwise(), filter(([[prevErrorState, prevDescribedBy], [currentErrorState, currentDescribedBy]]) => {
			return prevErrorState !== currentErrorState || prevDescribedBy !== currentDescribedBy;
		})).subscribe(() => this._syncDescribedByIds());
		this._valueChanges?.unsubscribe();
		if (control.ngControl && control.ngControl.valueChanges && !isFieldTree(control.ngField)) this._valueChanges = control.ngControl.valueChanges.pipe(takeUntil(this._destroyed)).subscribe(() => this._changeDetectorRef.markForCheck());
	}
	_checkPrefixAndSuffixTypes() {
		this._hasIconPrefix = !!this._prefixChildren.find((p) => !p._isText);
		this._hasTextPrefix = !!this._prefixChildren.find((p) => p._isText);
		this._hasIconSuffix = !!this._suffixChildren.find((s) => !s._isText);
		this._hasTextSuffix = !!this._suffixChildren.find((s) => s._isText);
	}
	_initializePrefixAndSuffix() {
		this._checkPrefixAndSuffixTypes();
		merge(this._prefixChildren.changes, this._suffixChildren.changes).subscribe(() => {
			this._checkPrefixAndSuffixTypes();
			this._changeDetectorRef.markForCheck();
		});
	}
	_initializeSubscript() {
		this._hintChildren.changes.subscribe(() => {
			this._processHints();
			this._changeDetectorRef.markForCheck();
		});
		this._errorChildren.changes.subscribe(() => {
			this._syncDescribedByIds();
			this._changeDetectorRef.markForCheck();
		});
		this._validateHints();
		this._syncDescribedByIds();
	}
	_assertFormFieldControl() {
		if (!this._control && (typeof ngDevMode === "undefined" || ngDevMode)) throw getMatFormFieldMissingControlError();
	}
	_updateFocusState() {
		const controlFocused = this._unwrapMaybeSignal(this._control.focused);
		if (controlFocused && !this._isFocused) {
			this._isFocused = true;
			this._lineRipple?.activate();
		} else if (!controlFocused && (this._isFocused || this._isFocused === null)) {
			this._isFocused = false;
			this._lineRipple?.deactivate();
		}
		this._elementRef.nativeElement.classList.toggle("mat-focused", controlFocused);
		this._textField?.nativeElement.classList.toggle("mdc-text-field--focused", controlFocused);
	}
	_syncOutlineLabelOffset() {
		afterRenderEffect({
			earlyRead: () => {
				if (this._appearanceSignal() !== "outline") {
					this._outlineLabelOffsetResizeObserver?.disconnect();
					return null;
				}
				if (globalThis.ResizeObserver) {
					this._outlineLabelOffsetResizeObserver ||= new globalThis.ResizeObserver(() => {
						this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset());
					});
					for (const el of this._prefixSuffixContainers()) this._outlineLabelOffsetResizeObserver.observe(el, { box: "border-box" });
				}
				return this._getOutlinedLabelOffset();
			},
			write: (labelStyles) => this._writeOutlinedLabelStyles(labelStyles())
		});
	}
	_shouldAlwaysFloat() {
		return this.floatLabel === "always";
	}
	_hasOutline() {
		return this.appearance === "outline";
	}
	_forceDisplayInfixLabel() {
		return !this._platform.isBrowser && this._prefixChildren.length && !this._shouldLabelFloat();
	}
	_hasFloatingLabel = computed(() => !!this._labelChild(), ...ngDevMode ? [{ debugName: "_hasFloatingLabel" }] : []);
	_shouldLabelFloat() {
		if (!this._hasFloatingLabel()) return false;
		return this._shouldAlwaysFloat() || this._unwrapMaybeSignal(this._control.shouldLabelFloat);
	}
	_shouldForward(prop) {
		const rawControl = this._control?.ngField || this._control?.ngControl;
		if (!rawControl) return false;
		if (isFieldTree(rawControl)) {
			const control = rawControl();
			if (prop === "valid") return control.valid();
			else if (prop === "dirty") return control.dirty();
			else if (prop === "touched") return control.touched();
			else if (prop === "pending") return control.pending();
			else if (prop === "untouched") return !control.touched();
			else if (prop === "pristine") return !control.dirty();
			else if (prop === "invalid") return !control.valid();
			return false;
		} else {
			const control = rawControl;
			if (prop === "valid") return control.valid;
			else if (prop === "dirty") return control.dirty;
			else if (prop === "touched") return control.touched;
			else if (prop === "pending") return control.pending;
			else if (prop === "untouched") return control.untouched;
			else if (prop === "pristine") return control.pristine;
			else if (prop === "invalid") return control.invalid;
			return false;
		}
	}
	_getSubscriptMessageType() {
		return this._errorChildren && this._errorChildren.length > 0 && this._unwrapMaybeSignal(this._control.errorState) ? "error" : "hint";
	}
	_handleLabelResized() {
		this._refreshOutlineNotchWidth();
	}
	_refreshOutlineNotchWidth() {
		if (!this._hasOutline() || !this._floatingLabel || !this._shouldLabelFloat()) this._notchedOutline?._setNotchWidth(0);
		else this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth());
	}
	_processHints() {
		this._validateHints();
		this._syncDescribedByIds();
	}
	_validateHints() {
		if (this._hintChildren && (typeof ngDevMode === "undefined" || ngDevMode)) {
			let startHint;
			let endHint;
			this._hintChildren.forEach((hint) => {
				if (hint.align === "start") {
					if (startHint || this.hintLabel) throw getMatFormFieldDuplicatedHintError("start");
					startHint = hint;
				} else if (hint.align === "end") {
					if (endHint) throw getMatFormFieldDuplicatedHintError("end");
					endHint = hint;
				}
			});
		}
	}
	_syncDescribedByIds() {
		if (this._control) {
			let ids = [];
			if (this._control.userAriaDescribedBy && typeof this._control.userAriaDescribedBy === "string") ids.push(...this._control.userAriaDescribedBy.split(" "));
			if (this._getSubscriptMessageType() === "hint") {
				const startHint = this._hintChildren ? this._hintChildren.find((hint) => hint.align === "start") : null;
				const endHint = this._hintChildren ? this._hintChildren.find((hint) => hint.align === "end") : null;
				if (startHint) ids.push(startHint.id);
				else if (this._hintLabel) ids.push(this._hintLabelId);
				if (endHint) ids.push(endHint.id);
			} else if (this._errorChildren) ids.push(...this._errorChildren.map((error) => error.id));
			const existingDescribedBy = this._control.describedByIds;
			let toAssign;
			if (existingDescribedBy) {
				const exclude = this._describedByIds || ids;
				toAssign = ids.concat(existingDescribedBy.filter((id) => id && !exclude.includes(id)));
			} else toAssign = ids;
			this._control.setDescribedByIds(toAssign);
			this._describedByIds = ids;
		}
	}
	_getOutlinedLabelOffset() {
		if (!this._hasOutline() || !this._floatingLabel) return null;
		if (!this._iconPrefixContainer && !this._textPrefixContainer) return ["", null];
		if (!this._isAttachedToDom()) return null;
		const iconPrefixContainer = this._iconPrefixContainer?.nativeElement;
		const textPrefixContainer = this._textPrefixContainer?.nativeElement;
		const iconSuffixContainer = this._iconSuffixContainer?.nativeElement;
		const textSuffixContainer = this._textSuffixContainer?.nativeElement;
		const iconPrefixContainerWidth = iconPrefixContainer?.getBoundingClientRect().width ?? 0;
		const textPrefixContainerWidth = textPrefixContainer?.getBoundingClientRect().width ?? 0;
		const iconSuffixContainerWidth = iconSuffixContainer?.getBoundingClientRect().width ?? 0;
		const textSuffixContainerWidth = textSuffixContainer?.getBoundingClientRect().width ?? 0;
		return [`var(--mat-mdc-form-field-label-transform, ${FLOATING_LABEL_DEFAULT_DOCKED_TRANSFORM} translateX(${`calc(${this._currentDirection === "rtl" ? "-1" : "1"} * (${`${iconPrefixContainerWidth + textPrefixContainerWidth}px`} + var(--mat-mdc-form-field-label-offset-x, 0px)))`}))`, iconPrefixContainerWidth + textPrefixContainerWidth + iconSuffixContainerWidth + textSuffixContainerWidth];
	}
	_writeOutlinedLabelStyles(styles) {
		if (styles !== null) {
			const [floatingLabelTransform, notchedOutlineWidth] = styles;
			if (this._floatingLabel) this._floatingLabel.element.style.transform = floatingLabelTransform;
			if (notchedOutlineWidth !== null) this._notchedOutline?._setMaxWidth(notchedOutlineWidth);
		}
	}
	_isAttachedToDom() {
		const element = this._elementRef.nativeElement;
		if (element.getRootNode) {
			const rootNode = element.getRootNode();
			return rootNode && rootNode !== element;
		}
		return document.documentElement.contains(element);
	}
	static ɵfac = function MatFormField_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatFormField)();
	};
	static ɵcmp = (function() {
		const _c0 = ["iconPrefixContainer"];
		const _c1 = ["textPrefixContainer"];
		const _c2 = ["iconSuffixContainer"];
		const _c3 = ["textSuffixContainer"];
		const _c4 = ["textField"];
		const _c5 = [
			"*",
			[["mat-label"]],
			[[
				"",
				"matPrefix",
				""
			], [
				"",
				"matIconPrefix",
				""
			]],
			[[
				"",
				"matTextPrefix",
				""
			]],
			[[
				"",
				"matTextSuffix",
				""
			]],
			[[
				"",
				"matSuffix",
				""
			], [
				"",
				"matIconSuffix",
				""
			]],
			[["mat-error"], [
				"",
				"matError",
				""
			]],
			[[
				"mat-hint",
				3,
				"align",
				"end"
			]],
			[[
				"mat-hint",
				"align",
				"end"
			]]
		];
		const _c6 = [
			"*",
			"mat-label",
			"[matPrefix], [matIconPrefix]",
			"[matTextPrefix]",
			"[matTextSuffix]",
			"[matSuffix], [matIconSuffix]",
			"mat-error, [matError]",
			"mat-hint:not([align='end'])",
			"mat-hint[align='end']"
		];
		function MatFormField_ng_template_0_Conditional_0_Conditional_2_Template(rf, ctx) {
			if (rf & 1) ɵɵelement(0, "span", 21);
		}
		function MatFormField_ng_template_0_Conditional_0_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "label", 20);
				ɵɵprojection(1, 1);
				ɵɵconditionalCreate(2, MatFormField_ng_template_0_Conditional_0_Conditional_2_Template, 1, 0, "span", 21);
				ɵɵelementEnd();
			}
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext(2);
				ɵɵproperty("floating", ctx_r0._shouldLabelFloat())("monitorResize", ctx_r0._hasOutline())("id", ctx_r0._labelId);
				ɵɵattribute("for", ctx_r0._control.disableAutomaticLabeling ? null : ctx_r0._control.id);
				ɵɵadvance(2);
				ɵɵconditional(!ctx_r0.hideRequiredMarker && ctx_r0._unwrapMaybeSignal(ctx_r0._control.required) ? 2 : -1);
			}
		}
		function MatFormField_ng_template_0_Template(rf, ctx) {
			if (rf & 1) ɵɵconditionalCreate(0, MatFormField_ng_template_0_Conditional_0_Template, 3, 5, "label", 20);
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext();
				ɵɵconditional(ctx_r0._hasFloatingLabel() ? 0 : -1);
			}
		}
		function MatFormField_Conditional_4_Template(rf, ctx) {
			if (rf & 1) ɵɵelement(0, "div", 7);
		}
		function MatFormField_Conditional_6_Conditional_1_ng_template_0_Template(rf, ctx) {}
		function MatFormField_Conditional_6_Conditional_1_Template(rf, ctx) {
			if (rf & 1) ɵɵtemplate(0, MatFormField_Conditional_6_Conditional_1_ng_template_0_Template, 0, 0, "ng-template", 13);
			if (rf & 2) {
				ɵɵnextContext(2);
				const labelTemplate_r2 = ɵɵreference(1);
				ɵɵproperty("ngTemplateOutlet", labelTemplate_r2);
			}
		}
		function MatFormField_Conditional_6_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 9);
				ɵɵconditionalCreate(1, MatFormField_Conditional_6_Conditional_1_Template, 1, 1, null, 13);
				ɵɵelementEnd();
			}
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext();
				ɵɵproperty("matFormFieldNotchedOutlineOpen", ctx_r0._shouldLabelFloat());
				ɵɵadvance();
				ɵɵconditional(!ctx_r0._forceDisplayInfixLabel() ? 1 : -1);
			}
		}
		function MatFormField_Conditional_7_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 10, 2);
				ɵɵprojection(2, 2);
				ɵɵelementEnd();
			}
		}
		function MatFormField_Conditional_8_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 11, 3);
				ɵɵprojection(2, 3);
				ɵɵelementEnd();
			}
		}
		function MatFormField_Conditional_10_ng_template_0_Template(rf, ctx) {}
		function MatFormField_Conditional_10_Template(rf, ctx) {
			if (rf & 1) ɵɵtemplate(0, MatFormField_Conditional_10_ng_template_0_Template, 0, 0, "ng-template", 13);
			if (rf & 2) {
				ɵɵnextContext();
				const labelTemplate_r2 = ɵɵreference(1);
				ɵɵproperty("ngTemplateOutlet", labelTemplate_r2);
			}
		}
		function MatFormField_Conditional_12_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 14, 4);
				ɵɵprojection(2, 4);
				ɵɵelementEnd();
			}
		}
		function MatFormField_Conditional_13_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 15, 5);
				ɵɵprojection(2, 5);
				ɵɵelementEnd();
			}
		}
		function MatFormField_Conditional_14_Template(rf, ctx) {
			if (rf & 1) ɵɵelement(0, "div", 16);
		}
		function MatFormField_Case_16_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 18);
				ɵɵprojection(1, 6);
				ɵɵelementEnd();
			}
		}
		function MatFormField_Case_17_Conditional_1_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "mat-hint", 22);
				ɵɵtext(1);
				ɵɵelementEnd();
			}
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext(2);
				ɵɵproperty("id", ctx_r0._hintLabelId);
				ɵɵadvance();
				ɵɵtextInterpolate(ctx_r0.hintLabel);
			}
		}
		function MatFormField_Case_17_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "div", 19);
				ɵɵconditionalCreate(1, MatFormField_Case_17_Conditional_1_Template, 2, 2, "mat-hint", 22);
				ɵɵprojection(2, 7);
				ɵɵelement(3, "div", 23);
				ɵɵprojection(4, 8);
				ɵɵelementEnd();
			}
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext();
				ɵɵadvance();
				ɵɵconditional(ctx_r0.hintLabel ? 1 : -1);
			}
		}
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatFormField,
			selectors: [["mat-form-field"]],
			contentQueries: function MatFormField_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) {
					ɵɵcontentQuerySignal(dirIndex, ctx._labelChild, MatLabel, 5);
					ɵɵcontentQuery(dirIndex, MatFormFieldControl, 5)(dirIndex, MAT_PREFIX, 5)(dirIndex, MAT_SUFFIX, 5)(dirIndex, MAT_ERROR, 5)(dirIndex, MatHint, 5);
				}
				if (rf & 2) {
					ɵɵqueryAdvance();
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._formFieldControl = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._prefixChildren = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._suffixChildren = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._errorChildren = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._hintChildren = _t);
				}
			},
			viewQuery: function MatFormField_Query(rf, ctx) {
				if (rf & 1) {
					ɵɵviewQuerySignal(ctx._iconPrefixContainerSignal, _c0, 5)(ctx._textPrefixContainerSignal, _c1, 5)(ctx._iconSuffixContainerSignal, _c2, 5)(ctx._textSuffixContainerSignal, _c3, 5);
					ɵɵviewQuery(_c4, 5)(_c0, 5)(_c1, 5)(_c2, 5)(_c3, 5)(MatFormFieldFloatingLabel, 5)(MatFormFieldNotchedOutline, 5)(MatFormFieldLineRipple, 5);
				}
				if (rf & 2) {
					ɵɵqueryAdvance(4);
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._textField = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._iconPrefixContainer = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._textPrefixContainer = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._iconSuffixContainer = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._textSuffixContainer = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._floatingLabel = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._notchedOutline = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._lineRipple = _t.first);
				}
			},
			hostAttrs: [1, "mat-mdc-form-field"],
			hostVars: 38,
			hostBindings: function MatFormField_HostBindings(rf, ctx) {
				if (rf & 2) ɵɵclassProp("mat-mdc-form-field-label-always-float", ctx._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix", ctx._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix", ctx._hasIconSuffix)("mat-form-field-invalid", ctx._unwrapMaybeSignal(ctx._control.errorState))("mat-form-field-disabled", ctx._unwrapMaybeSignal(ctx._control.disabled))("mat-form-field-autofilled", ctx._unwrapMaybeSignal(ctx._control.autofilled))("mat-form-field-appearance-fill", ctx.appearance == "fill")("mat-form-field-appearance-outline", ctx.appearance == "outline")("mat-form-field-hide-placeholder", ctx._hasFloatingLabel() && !ctx._shouldLabelFloat())("mat-primary", ctx.color !== "accent" && ctx.color !== "warn")("mat-accent", ctx.color === "accent")("mat-warn", ctx.color === "warn")("ng-untouched", ctx._shouldForward("untouched"))("ng-touched", ctx._shouldForward("touched"))("ng-pristine", ctx._shouldForward("pristine"))("ng-dirty", ctx._shouldForward("dirty"))("ng-valid", ctx._shouldForward("valid"))("ng-invalid", ctx._shouldForward("invalid"))("ng-pending", ctx._shouldForward("pending"));
			},
			inputs: {
				hideRequiredMarker: "hideRequiredMarker",
				color: "color",
				floatLabel: "floatLabel",
				appearance: "appearance",
				subscriptSizing: "subscriptSizing",
				hintLabel: "hintLabel"
			},
			exportAs: ["matFormField"],
			features: [ɵɵProvidersFeature([{
				provide: MAT_FORM_FIELD,
				useExisting: MatFormField
			}, {
				provide: FLOATING_LABEL_PARENT,
				useExisting: MatFormField
			}])],
			ngContentSelectors: _c6,
			decls: 18,
			vars: 21,
			consts: [
				["labelTemplate", ""],
				["textField", ""],
				["iconPrefixContainer", ""],
				["textPrefixContainer", ""],
				["textSuffixContainer", ""],
				["iconSuffixContainer", ""],
				[
					1,
					"mat-mdc-text-field-wrapper",
					"mdc-text-field",
					3,
					"click"
				],
				[1, "mat-mdc-form-field-focus-overlay"],
				[1, "mat-mdc-form-field-flex"],
				[
					"matFormFieldNotchedOutline",
					"",
					3,
					"matFormFieldNotchedOutlineOpen"
				],
				[1, "mat-mdc-form-field-icon-prefix"],
				[1, "mat-mdc-form-field-text-prefix"],
				[1, "mat-mdc-form-field-infix"],
				[3, "ngTemplateOutlet"],
				[1, "mat-mdc-form-field-text-suffix"],
				[1, "mat-mdc-form-field-icon-suffix"],
				["matFormFieldLineRipple", ""],
				[
					"aria-atomic",
					"true",
					"aria-live",
					"polite",
					1,
					"mat-mdc-form-field-subscript-wrapper",
					"mat-mdc-form-field-bottom-align"
				],
				[1, "mat-mdc-form-field-error-wrapper"],
				[1, "mat-mdc-form-field-hint-wrapper"],
				[
					"matFormFieldFloatingLabel",
					"",
					3,
					"floating",
					"monitorResize",
					"id"
				],
				[
					"aria-hidden",
					"true",
					1,
					"mat-mdc-form-field-required-marker",
					"mdc-floating-label--required"
				],
				[3, "id"],
				[1, "mat-mdc-form-field-hint-spacer"]
			],
			template: function MatFormField_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef(_c5);
					ɵɵtemplate(0, MatFormField_ng_template_0_Template, 1, 1, "ng-template", null, 0, ɵɵtemplateRefExtractor);
					ɵɵelementStart(2, "div", 6, 1);
					ɵɵlistener("click", function MatFormField_Template_div_click_2_listener($event) {
						return ctx._control.onContainerClick($event);
					});
					ɵɵconditionalCreate(4, MatFormField_Conditional_4_Template, 1, 0, "div", 7);
					ɵɵelementStart(5, "div", 8);
					ɵɵconditionalCreate(6, MatFormField_Conditional_6_Template, 2, 2, "div", 9);
					ɵɵconditionalCreate(7, MatFormField_Conditional_7_Template, 3, 0, "div", 10);
					ɵɵconditionalCreate(8, MatFormField_Conditional_8_Template, 3, 0, "div", 11);
					ɵɵelementStart(9, "div", 12);
					ɵɵconditionalCreate(10, MatFormField_Conditional_10_Template, 1, 1, null, 13);
					ɵɵprojection(11);
					ɵɵelementEnd();
					ɵɵconditionalCreate(12, MatFormField_Conditional_12_Template, 3, 0, "div", 14);
					ɵɵconditionalCreate(13, MatFormField_Conditional_13_Template, 3, 0, "div", 15);
					ɵɵelementEnd();
					ɵɵconditionalCreate(14, MatFormField_Conditional_14_Template, 1, 0, "div", 16);
					ɵɵelementEnd();
					ɵɵelementStart(15, "div", 17);
					ɵɵconditionalCreate(16, MatFormField_Case_16_Template, 2, 0, "div", 18)(17, MatFormField_Case_17_Template, 5, 1, "div", 19);
					ɵɵelementEnd();
				}
				if (rf & 2) {
					let tmp_18_0 = void 0;
					const isDisabled_r3 = ctx._unwrapMaybeSignal(ctx._control.disabled);
					ɵɵadvance(2);
					ɵɵclassProp("mdc-text-field--filled", !ctx._hasOutline())("mdc-text-field--outlined", ctx._hasOutline())("mdc-text-field--no-label", !ctx._hasFloatingLabel())("mdc-text-field--disabled", isDisabled_r3)("mdc-text-field--invalid", ctx._unwrapMaybeSignal(ctx._control.errorState));
					ɵɵadvance(2);
					ɵɵconditional(!ctx._hasOutline() && !isDisabled_r3 ? 4 : -1);
					ɵɵadvance(2);
					ɵɵconditional(ctx._hasOutline() ? 6 : -1);
					ɵɵadvance();
					ɵɵconditional(ctx._hasIconPrefix ? 7 : -1);
					ɵɵadvance();
					ɵɵconditional(ctx._hasTextPrefix ? 8 : -1);
					ɵɵadvance(2);
					ɵɵconditional(!ctx._hasOutline() || ctx._forceDisplayInfixLabel() ? 10 : -1);
					ɵɵadvance(2);
					ɵɵconditional(ctx._hasTextSuffix ? 12 : -1);
					ɵɵadvance();
					ɵɵconditional(ctx._hasIconSuffix ? 13 : -1);
					ɵɵadvance();
					ɵɵconditional(!ctx._hasOutline() ? 14 : -1);
					ɵɵadvance();
					ɵɵclassProp("mat-mdc-form-field-subscript-dynamic-size", ctx.subscriptSizing === "dynamic");
					const subscriptMessageType_r4 = ctx._getSubscriptMessageType();
					ɵɵadvance();
					ɵɵconditional((tmp_18_0 = subscriptMessageType_r4) === "error" ? 16 : tmp_18_0 === "hint" ? 17 : -1);
				}
			},
			dependencies: [
				MatFormFieldFloatingLabel,
				MatFormFieldNotchedOutline,
				NgTemplateOutlet,
				MatFormFieldLineRipple,
				MatHint
			],
			styles: [".mdc-text-field {\n  display: inline-flex;\n  align-items: baseline;\n  padding: 0 16px;\n  position: relative;\n  box-sizing: border-box;\n  overflow: hidden;\n  will-change: opacity, transform, color;\n  border-top-left-radius: 4px;\n  border-top-right-radius: 4px;\n  border-bottom-right-radius: 0;\n  border-bottom-left-radius: 0;\n}\n\n.mdc-text-field__input {\n  width: 100%;\n  min-width: 0;\n  border: none;\n  border-radius: 0;\n  background: none;\n  padding: 0;\n  -moz-appearance: none;\n  -webkit-appearance: none;\n  height: 28px;\n}\n.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {\n  display: none;\n}\n.mdc-text-field__input::-ms-clear {\n  display: none;\n}\n.mdc-text-field__input:focus {\n  outline: none;\n}\n.mdc-text-field__input:invalid {\n  box-shadow: none;\n}\n.mdc-text-field__input::placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input::-moz-placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input::-webkit-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input:-ms-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {\n  opacity: 0;\n}\n.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {\n  height: 100%;\n}\n.mdc-text-field--outlined .mdc-text-field__input {\n  display: flex;\n  border: none !important;\n  background-color: transparent;\n}\n.mdc-text-field--disabled .mdc-text-field__input {\n  pointer-events: auto;\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));\n  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {\n  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {\n  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));\n  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {\n  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {\n  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {\n  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {\n  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n@media (forced-colors: active) {\n  .mdc-text-field--disabled .mdc-text-field__input {\n    background-color: Window;\n  }\n}\n\n.mdc-text-field--filled {\n  height: 56px;\n  border-bottom-right-radius: 0;\n  border-bottom-left-radius: 0;\n  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));\n  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {\n  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--disabled {\n  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));\n}\n\n.mdc-text-field--outlined {\n  height: 56px;\n  overflow: visible;\n  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));\n  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);\n}\n[dir=rtl] .mdc-text-field--outlined {\n  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);\n  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));\n}\n\n.mdc-floating-label {\n  position: absolute;\n  left: 0;\n  transform-origin: left top;\n  line-height: 1.15rem;\n  text-align: left;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n  cursor: text;\n  overflow: hidden;\n  will-change: transform;\n}\n[dir=rtl] .mdc-floating-label {\n  right: 0;\n  left: auto;\n  transform-origin: right top;\n  text-align: right;\n}\n.mdc-text-field .mdc-floating-label {\n  top: 50%;\n  transform: translateY(-50%);\n  pointer-events: none;\n}\n.mdc-notched-outline .mdc-floating-label {\n  display: inline-block;\n  position: relative;\n  max-width: 100%;\n}\n.mdc-text-field--outlined .mdc-floating-label {\n  left: 4px;\n  right: auto;\n}\n[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {\n  left: auto;\n  right: 4px;\n}\n.mdc-text-field--filled .mdc-floating-label {\n  left: 16px;\n  right: auto;\n}\n[dir=rtl] .mdc-text-field--filled .mdc-floating-label {\n  left: auto;\n  right: 16px;\n}\n.mdc-text-field--disabled .mdc-floating-label {\n  cursor: default;\n}\n@media (forced-colors: active) {\n  .mdc-text-field--disabled .mdc-floating-label {\n    z-index: 1;\n  }\n}\n.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {\n  display: none;\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {\n  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));\n}\n.mdc-text-field--filled .mdc-floating-label {\n  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));\n  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));\n  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));\n  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {\n  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));\n}\n.mdc-text-field--outlined .mdc-floating-label {\n  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));\n  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));\n  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));\n  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));\n}\n\n.mdc-floating-label--float-above {\n  cursor: auto;\n  transform: translateY(-106%) scale(0.75);\n}\n.mdc-text-field--filled .mdc-floating-label--float-above {\n  transform: translateY(-106%) scale(0.75);\n}\n.mdc-text-field--outlined .mdc-floating-label--float-above {\n  transform: translateY(-37.25px) scale(1);\n  font-size: 0.75rem;\n}\n.mdc-notched-outline .mdc-floating-label--float-above {\n  text-overflow: clip;\n}\n.mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  max-width: 133.3333333333%;\n}\n.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  transform: translateY(-34.75px) scale(0.75);\n}\n.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  font-size: 1rem;\n}\n\n.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {\n  margin-left: 1px;\n  margin-right: 0;\n  content: \"*\";\n}\n[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {\n  margin-left: 0;\n  margin-right: 1px;\n}\n\n.mdc-notched-outline {\n  display: flex;\n  position: absolute;\n  top: 0;\n  right: 0;\n  left: 0;\n  box-sizing: border-box;\n  width: 100%;\n  max-width: 100%;\n  height: 100%;\n  text-align: left;\n  pointer-events: none;\n}\n[dir=rtl] .mdc-notched-outline {\n  text-align: right;\n}\n.mdc-text-field--outlined .mdc-notched-outline {\n  z-index: 1;\n}\n\n.mat-mdc-notch-piece {\n  box-sizing: border-box;\n  height: 100%;\n  pointer-events: none;\n  border: none;\n  border-top: 1px solid;\n  border-bottom: 1px solid;\n}\n.mdc-text-field--focused .mat-mdc-notch-piece {\n  border-width: 2px;\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));\n  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {\n  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {\n  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);\n}\n\n.mdc-notched-outline__leading {\n  border-left: 1px solid;\n  border-right: none;\n  border-top-right-radius: 0;\n  border-bottom-right-radius: 0;\n  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n}\n.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {\n  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));\n}\n[dir=rtl] .mdc-notched-outline__leading {\n  border-left: none;\n  border-right: 1px solid;\n  border-bottom-left-radius: 0;\n  border-top-left-radius: 0;\n  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n}\n\n.mdc-notched-outline__trailing {\n  flex-grow: 1;\n  border-left: none;\n  border-right: 1px solid;\n  border-top-left-radius: 0;\n  border-bottom-left-radius: 0;\n  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n}\n[dir=rtl] .mdc-notched-outline__trailing {\n  border-left: 1px solid;\n  border-right: none;\n  border-top-right-radius: 0;\n  border-bottom-right-radius: 0;\n  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));\n}\n\n.mdc-notched-outline__notch {\n  flex: 0 0 auto;\n  width: auto;\n}\n.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {\n  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));\n}\n.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));\n}\n.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-top: 1px;\n}\n.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-top: 2px;\n}\n.mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-left: 0;\n  padding-right: 8px;\n  border-top: none;\n}\n[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-left: 8px;\n  padding-right: 0;\n}\n.mdc-notched-outline--no-label .mdc-notched-outline__notch {\n  display: none;\n}\n\n.mdc-line-ripple::before, .mdc-line-ripple::after {\n  position: absolute;\n  bottom: 0;\n  left: 0;\n  width: 100%;\n  border-bottom-style: solid;\n  content: \"\";\n}\n.mdc-line-ripple::before {\n  z-index: 1;\n  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {\n  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {\n  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {\n  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {\n  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {\n  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));\n}\n.mdc-line-ripple::after {\n  transform: scaleX(0);\n  opacity: 0;\n  z-index: 2;\n}\n.mdc-text-field--filled .mdc-line-ripple::after {\n  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);\n}\n.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {\n  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));\n}\n.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {\n  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));\n}\n\n.mdc-line-ripple--%NS%active::after {\n  transform: scaleX(1);\n  opacity: 1;\n}\n\n.mdc-line-ripple--%NS%deactivating::after {\n  opacity: 0;\n}\n\n.mdc-text-field--disabled {\n  pointer-events: none;\n}\n\n.mat-mdc-form-field-textarea-control {\n  vertical-align: middle;\n  resize: vertical;\n  box-sizing: border-box;\n  height: auto;\n  margin: 0;\n  padding: 0;\n  border: none;\n  overflow: auto;\n}\n\n.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font: inherit;\n  letter-spacing: inherit;\n  text-decoration: inherit;\n  text-transform: inherit;\n  border: none;\n}\n\n.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  line-height: normal;\n  pointer-events: all;\n  will-change: auto;\n}\n\n.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {\n  cursor: inherit;\n}\n\n.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {\n  height: auto;\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {\n  height: 23px;\n}\n\n.mat-mdc-text-field-wrapper {\n  height: auto;\n  flex: auto;\n  will-change: auto;\n}\n\n.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {\n  padding-left: 0;\n  --%NS%mat-mdc-form-field-label-offset-x: -16px;\n}\n\n.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {\n  padding-right: 0;\n}\n\n[dir=rtl] .mat-mdc-text-field-wrapper {\n  padding-left: 16px;\n  padding-right: 16px;\n}\n[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {\n  padding-left: 0;\n}\n[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {\n  padding-right: 0;\n}\n\n.mat-form-field-disabled .mdc-text-field__input::placeholder {\n  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {\n  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n\n.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n  opacity: 1;\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {\n  left: auto;\n  right: auto;\n}\n\n.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {\n  display: inline-block;\n}\n\n.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {\n  padding-top: 0;\n}\n\n.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {\n  border-left: 1px solid transparent;\n}\n\n[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {\n  border-left: none;\n  border-right: 1px solid transparent;\n}\n\n.mat-mdc-form-field-infix {\n  min-height: var(--%NS%mat-form-field-container-height, 56px);\n  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);\n  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);\n}\n.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {\n  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);\n  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {\n  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);\n}\n\n.mdc-text-field--filled .mat-mdc-floating-label {\n  display: var(--%NS%mat-form-field-filled-label-display, block);\n}\n\n.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))\n    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));\n  transform: var(--%NS%mat-mdc-form-field-label-transform);\n}\n\n@keyframes _mat-form-field-subscript-animation {\n  from {\n    opacity: 0;\n    transform: translateY(-5px);\n  }\n  to {\n    opacity: 1;\n    transform: translateY(0);\n  }\n}\n.mat-mdc-form-field-subscript-wrapper {\n  box-sizing: border-box;\n  width: 100%;\n  position: relative;\n}\n\n.mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field-error-wrapper {\n  position: absolute;\n  top: 0;\n  left: 0;\n  right: 0;\n  padding: 0 16px;\n  opacity: 1;\n  transform: translateY(0);\n  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);\n}\n\n.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {\n  position: static;\n}\n\n.mat-mdc-form-field-bottom-align::before {\n  content: \"\";\n  display: inline-block;\n  height: 16px;\n}\n\n.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {\n  content: unset;\n}\n\n.mat-mdc-form-field-hint-end {\n  order: 1;\n}\n\n.mat-mdc-form-field-hint-wrapper {\n  display: flex;\n}\n\n.mat-mdc-form-field-hint-spacer {\n  flex: 1 0 1em;\n}\n\n.mat-mdc-form-field-error {\n  display: block;\n  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));\n}\n\n.mat-mdc-form-field-subscript-wrapper,\n.mat-mdc-form-field-bottom-align::before {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));\n  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));\n  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));\n  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));\n  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));\n}\n\n.mat-mdc-form-field-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  opacity: 0;\n  pointer-events: none;\n  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));\n}\n.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {\n  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {\n  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);\n}\n\nselect.mat-mdc-form-field-input-control {\n  -moz-appearance: none;\n  -webkit-appearance: none;\n  background-color: transparent;\n  display: inline-flex;\n  box-sizing: border-box;\n}\nselect.mat-mdc-form-field-input-control:not(:disabled) {\n  cursor: pointer;\n}\nselect.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {\n  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));\n}\nselect.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {\n  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));\n}\n\n.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {\n  content: \"\";\n  width: 0;\n  height: 0;\n  border-left: 5px solid transparent;\n  border-right: 5px solid transparent;\n  border-top: 5px solid;\n  position: absolute;\n  right: 0;\n  top: 50%;\n  margin-top: -2.5px;\n  pointer-events: none;\n  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));\n}\n[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {\n  right: auto;\n  left: 0;\n}\n.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {\n  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));\n}\n.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {\n  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {\n  padding-right: 15px;\n}\n[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {\n  padding-right: 0;\n  padding-left: 15px;\n}\n\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {\n    outline: solid 1px;\n  }\n}\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {\n    outline-color: GrayText;\n  }\n}\n\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {\n    outline: dashed 3px;\n  }\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-form-field.mat-focused .mdc-notched-outline {\n    border: dashed 3px;\n  }\n}\n\n.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {\n  line-height: 1;\n}\n.mat-mdc-form-field-input-control::-webkit-datetime-edit {\n  line-height: 1;\n  padding: 0;\n  margin-bottom: -2px;\n}\n\n.mat-mdc-form-field {\n  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;\n  display: inline-flex;\n  flex-direction: column;\n  min-width: 0;\n  text-align: left;\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));\n  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));\n  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));\n  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));\n  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));\n}\n.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {\n  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));\n}\n.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);\n}\n[dir=rtl] .mat-mdc-form-field {\n  text-align: right;\n}\n\n.mat-mdc-form-field-flex {\n  display: inline-flex;\n  align-items: baseline;\n  box-sizing: border-box;\n  width: 100%;\n}\n\n.mat-mdc-text-field-wrapper {\n  width: 100%;\n  z-index: 0;\n}\n\n.mat-mdc-form-field-icon-prefix,\n.mat-mdc-form-field-icon-suffix {\n  align-self: center;\n  line-height: 0;\n  pointer-events: auto;\n  position: relative;\n  z-index: 1;\n}\n.mat-mdc-form-field-icon-prefix > .mat-icon,\n.mat-mdc-form-field-icon-suffix > .mat-icon {\n  padding: 0 12px;\n  box-sizing: content-box;\n}\n\n.mat-mdc-form-field-icon-prefix {\n  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {\n  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n\n.mat-mdc-form-field-icon-suffix {\n  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {\n  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {\n  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));\n}\n.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {\n  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));\n}\n.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {\n  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));\n}\n\n.mat-mdc-form-field-icon-prefix,\n[dir=rtl] .mat-mdc-form-field-icon-suffix {\n  padding: 0 4px 0 0;\n}\n\n.mat-mdc-form-field-icon-suffix,\n[dir=rtl] .mat-mdc-form-field-icon-prefix {\n  padding: 0 0 0 4px;\n}\n\n.mat-mdc-form-field-subscript-wrapper .mat-icon,\n.mat-mdc-form-field label .mat-icon {\n  width: 1em;\n  height: 1em;\n  font-size: inherit;\n}\n\n.mat-mdc-form-field-infix {\n  flex: auto;\n  min-width: 0;\n  width: 180px;\n  position: relative;\n  box-sizing: border-box;\n}\n.mat-mdc-form-field-infix:has(textarea[cols]) {\n  width: auto;\n}\n\n.mat-mdc-form-field .mdc-notched-outline__notch {\n  margin-left: -1px;\n  -webkit-clip-path: inset(-9em -999em -9em 1px);\n  clip-path: inset(-9em -999em -9em 1px);\n}\n[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {\n  margin-left: 0;\n  margin-right: -1px;\n  -webkit-clip-path: inset(-9em 1px -9em -999em);\n  clip-path: inset(-9em 1px -9em -999em);\n}\n\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {\n  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {\n  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {\n  transition-duration: 75ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {\n  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {\n  animation-duration: 300ms;\n}\n\n.mdc-notched-outline .mdc-floating-label {\n  max-width: calc(100% + 1px);\n}\n\n.mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  max-width: calc(133.3333333333% + 1px);\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatFormField, [{
		type: Component,
		args: [{
			selector: "mat-form-field",
			exportAs: "matFormField",
			host: {
				"class": "mat-mdc-form-field",
				"[class.mat-mdc-form-field-label-always-float]": "_shouldAlwaysFloat()",
				"[class.mat-mdc-form-field-has-icon-prefix]": "_hasIconPrefix",
				"[class.mat-mdc-form-field-has-icon-suffix]": "_hasIconSuffix",
				"[class.mat-form-field-invalid]": "_unwrapMaybeSignal(_control.errorState)",
				"[class.mat-form-field-disabled]": "_unwrapMaybeSignal(_control.disabled)",
				"[class.mat-form-field-autofilled]": "_unwrapMaybeSignal(_control.autofilled)",
				"[class.mat-form-field-appearance-fill]": "appearance == \"fill\"",
				"[class.mat-form-field-appearance-outline]": "appearance == \"outline\"",
				"[class.mat-form-field-hide-placeholder]": "_hasFloatingLabel() && !_shouldLabelFloat()",
				"[class.mat-primary]": "color !== \"accent\" && color !== \"warn\"",
				"[class.mat-accent]": "color === \"accent\"",
				"[class.mat-warn]": "color === \"warn\"",
				"[class.ng-untouched]": "_shouldForward(\"untouched\")",
				"[class.ng-touched]": "_shouldForward(\"touched\")",
				"[class.ng-pristine]": "_shouldForward(\"pristine\")",
				"[class.ng-dirty]": "_shouldForward(\"dirty\")",
				"[class.ng-valid]": "_shouldForward(\"valid\")",
				"[class.ng-invalid]": "_shouldForward(\"invalid\")",
				"[class.ng-pending]": "_shouldForward(\"pending\")"
			},
			encapsulation: ViewEncapsulation.None,
			providers: [{
				provide: MAT_FORM_FIELD,
				useExisting: MatFormField
			}, {
				provide: FLOATING_LABEL_PARENT,
				useExisting: MatFormField
			}],
			imports: [
				MatFormFieldFloatingLabel,
				MatFormFieldNotchedOutline,
				NgTemplateOutlet,
				MatFormFieldLineRipple,
				MatHint
			],
			template: "<ng-template #labelTemplate>\n  <!--\n    MDC recommends that the text-field is a `<label>` element. This rather complicates the\n    setup because it would require every form-field control to explicitly set `aria-labelledby`.\n    This is because the `<label>` itself contains more than the actual label (e.g. prefix, suffix\n    or other projected content), and screen readers could potentially read out undesired content.\n    Excluding elements from being printed out requires them to be marked with `aria-hidden`, or\n    the form control is set to a scoped element for the label (using `aria-labelledby`). Both of\n    these options seem to complicate the setup because we know exactly what content is rendered\n    as part of the label, and we don't want to spend resources on walking through projected content\n    to set `aria-hidden`. Nor do we want to set `aria-labelledby` on every form control if we could\n    simply link the label to the control using the label `for` attribute.\n  -->\n  @if (_hasFloatingLabel()) {\n    <label\n      matFormFieldFloatingLabel\n      [floating]=\"_shouldLabelFloat()\"\n      [monitorResize]=\"_hasOutline()\"\n      [id]=\"_labelId\"\n      [attr.for]=\"_control.disableAutomaticLabeling ? null : _control.id\"\n    >\n      <ng-content select=\"mat-label\"></ng-content>\n      <!--\n        We set the required marker as a separate element, in order to make it easier to target if\n        apps want to override it and to be able to set `aria-hidden` so that screen readers don't\n        pick it up.\n       -->\n      @if (!hideRequiredMarker && _unwrapMaybeSignal(_control.required)) {\n        <span\n          aria-hidden=\"true\"\n          class=\"mat-mdc-form-field-required-marker mdc-floating-label--required\"\n        ></span>\n      }\n    </label>\n  }\n</ng-template>\n\n@let isDisabled = _unwrapMaybeSignal(_control.disabled);\n\n<div\n  class=\"mat-mdc-text-field-wrapper mdc-text-field\"\n  #textField\n  [class.mdc-text-field--filled]=\"!_hasOutline()\"\n  [class.mdc-text-field--outlined]=\"_hasOutline()\"\n  [class.mdc-text-field--no-label]=\"!_hasFloatingLabel()\"\n  [class.mdc-text-field--disabled]=\"isDisabled\"\n  [class.mdc-text-field--invalid]=\"_unwrapMaybeSignal(_control.errorState)\"\n  (click)=\"_control.onContainerClick($event)\"\n>\n  @if (!_hasOutline() && !isDisabled) {\n    <div class=\"mat-mdc-form-field-focus-overlay\"></div>\n  }\n  <div class=\"mat-mdc-form-field-flex\">\n    @if (_hasOutline()) {\n      <div matFormFieldNotchedOutline [matFormFieldNotchedOutlineOpen]=\"_shouldLabelFloat()\">\n        @if (!_forceDisplayInfixLabel()) {\n          <ng-template [ngTemplateOutlet]=\"labelTemplate\"></ng-template>\n        }\n      </div>\n    }\n\n    @if (_hasIconPrefix) {\n      <div class=\"mat-mdc-form-field-icon-prefix\" #iconPrefixContainer>\n        <ng-content select=\"[matPrefix], [matIconPrefix]\"></ng-content>\n      </div>\n    }\n\n    @if (_hasTextPrefix) {\n      <div class=\"mat-mdc-form-field-text-prefix\" #textPrefixContainer>\n        <ng-content select=\"[matTextPrefix]\"></ng-content>\n      </div>\n    }\n\n    <div class=\"mat-mdc-form-field-infix\">\n      @if (!_hasOutline() || _forceDisplayInfixLabel()) {\n        <ng-template [ngTemplateOutlet]=\"labelTemplate\"></ng-template>\n      }\n\n      <ng-content></ng-content>\n    </div>\n\n    @if (_hasTextSuffix) {\n      <div class=\"mat-mdc-form-field-text-suffix\" #textSuffixContainer>\n        <ng-content select=\"[matTextSuffix]\"></ng-content>\n      </div>\n    }\n\n    @if (_hasIconSuffix) {\n      <div class=\"mat-mdc-form-field-icon-suffix\" #iconSuffixContainer>\n        <ng-content select=\"[matSuffix], [matIconSuffix]\"></ng-content>\n      </div>\n    }\n  </div>\n\n  @if (!_hasOutline()) {\n    <div matFormFieldLineRipple></div>\n  }\n</div>\n\n<div aria-atomic=\"true\" aria-live=\"polite\"\n  class=\"mat-mdc-form-field-subscript-wrapper mat-mdc-form-field-bottom-align\"\n  [class.mat-mdc-form-field-subscript-dynamic-size]=\"subscriptSizing === 'dynamic'\"\n>\n  @let subscriptMessageType = _getSubscriptMessageType();\n\n  @switch (subscriptMessageType) {\n    @case ('error') {\n      <div class=\"mat-mdc-form-field-error-wrapper\">\n        <ng-content select=\"mat-error, [matError]\"></ng-content>\n      </div>\n    }\n\n    @case ('hint') {\n      <div class=\"mat-mdc-form-field-hint-wrapper\">\n        @if (hintLabel) {\n          <mat-hint [id]=\"_hintLabelId\">{{hintLabel}}</mat-hint>\n        }\n        <ng-content select=\"mat-hint:not([align='end'])\"></ng-content>\n        <div class=\"mat-mdc-form-field-hint-spacer\"></div>\n        <ng-content select=\"mat-hint[align='end']\"></ng-content>\n      </div>\n    }\n  }\n</div>\n",
			styles: [".mdc-text-field {\n  display: inline-flex;\n  align-items: baseline;\n  padding: 0 16px;\n  position: relative;\n  box-sizing: border-box;\n  overflow: hidden;\n  will-change: opacity, transform, color;\n  border-top-left-radius: 4px;\n  border-top-right-radius: 4px;\n  border-bottom-right-radius: 0;\n  border-bottom-left-radius: 0;\n}\n\n.mdc-text-field__input {\n  width: 100%;\n  min-width: 0;\n  border: none;\n  border-radius: 0;\n  background: none;\n  padding: 0;\n  -moz-appearance: none;\n  -webkit-appearance: none;\n  height: 28px;\n}\n.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {\n  display: none;\n}\n.mdc-text-field__input::-ms-clear {\n  display: none;\n}\n.mdc-text-field__input:focus {\n  outline: none;\n}\n.mdc-text-field__input:invalid {\n  box-shadow: none;\n}\n.mdc-text-field__input::placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input::-moz-placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input::-webkit-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field__input:-ms-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {\n  opacity: 0;\n}\n.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {\n  opacity: 0;\n}\n.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {\n  height: 100%;\n}\n.mdc-text-field--outlined .mdc-text-field__input {\n  display: flex;\n  border: none !important;\n  background-color: transparent;\n}\n.mdc-text-field--disabled .mdc-text-field__input {\n  pointer-events: auto;\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));\n  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {\n  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {\n  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));\n  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {\n  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {\n  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));\n}\n.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {\n  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {\n  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {\n  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n@media (forced-colors: active) {\n  .mdc-text-field--disabled .mdc-text-field__input {\n    background-color: Window;\n  }\n}\n\n.mdc-text-field--filled {\n  height: 56px;\n  border-bottom-right-radius: 0;\n  border-bottom-left-radius: 0;\n  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));\n  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) {\n  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--disabled {\n  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));\n}\n\n.mdc-text-field--outlined {\n  height: 56px;\n  overflow: visible;\n  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));\n  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);\n}\n[dir=rtl] .mdc-text-field--outlined {\n  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);\n  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));\n}\n\n.mdc-floating-label {\n  position: absolute;\n  left: 0;\n  transform-origin: left top;\n  line-height: 1.15rem;\n  text-align: left;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n  cursor: text;\n  overflow: hidden;\n  will-change: transform;\n}\n[dir=rtl] .mdc-floating-label {\n  right: 0;\n  left: auto;\n  transform-origin: right top;\n  text-align: right;\n}\n.mdc-text-field .mdc-floating-label {\n  top: 50%;\n  transform: translateY(-50%);\n  pointer-events: none;\n}\n.mdc-notched-outline .mdc-floating-label {\n  display: inline-block;\n  position: relative;\n  max-width: 100%;\n}\n.mdc-text-field--outlined .mdc-floating-label {\n  left: 4px;\n  right: auto;\n}\n[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {\n  left: auto;\n  right: 4px;\n}\n.mdc-text-field--filled .mdc-floating-label {\n  left: 16px;\n  right: auto;\n}\n[dir=rtl] .mdc-text-field--filled .mdc-floating-label {\n  left: auto;\n  right: 16px;\n}\n.mdc-text-field--disabled .mdc-floating-label {\n  cursor: default;\n}\n@media (forced-colors: active) {\n  .mdc-text-field--disabled .mdc-floating-label {\n    z-index: 1;\n  }\n}\n.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {\n  display: none;\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {\n  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {\n  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {\n  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {\n  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {\n  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {\n  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {\n  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));\n}\n.mdc-text-field--filled .mdc-floating-label {\n  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));\n  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));\n  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));\n  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {\n  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {\n  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {\n  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {\n  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {\n  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {\n  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {\n  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));\n}\n.mdc-text-field--outlined .mdc-floating-label {\n  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));\n  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));\n  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));\n  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));\n}\n\n.mdc-floating-label--float-above {\n  cursor: auto;\n  transform: translateY(-106%) scale(0.75);\n}\n.mdc-text-field--filled .mdc-floating-label--float-above {\n  transform: translateY(-106%) scale(0.75);\n}\n.mdc-text-field--outlined .mdc-floating-label--float-above {\n  transform: translateY(-37.25px) scale(1);\n  font-size: 0.75rem;\n}\n.mdc-notched-outline .mdc-floating-label--float-above {\n  text-overflow: clip;\n}\n.mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  max-width: 133.3333333333%;\n}\n.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  transform: translateY(-34.75px) scale(0.75);\n}\n.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  font-size: 1rem;\n}\n\n.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {\n  margin-left: 1px;\n  margin-right: 0;\n  content: \"*\";\n}\n[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {\n  margin-left: 0;\n  margin-right: 1px;\n}\n\n.mdc-notched-outline {\n  display: flex;\n  position: absolute;\n  top: 0;\n  right: 0;\n  left: 0;\n  box-sizing: border-box;\n  width: 100%;\n  max-width: 100%;\n  height: 100%;\n  text-align: left;\n  pointer-events: none;\n}\n[dir=rtl] .mdc-notched-outline {\n  text-align: right;\n}\n.mdc-text-field--outlined .mdc-notched-outline {\n  z-index: 1;\n}\n\n.mat-mdc-notch-piece {\n  box-sizing: border-box;\n  height: 100%;\n  pointer-events: none;\n  border: none;\n  border-top: 1px solid;\n  border-bottom: 1px solid;\n}\n.mdc-text-field--focused .mat-mdc-notch-piece {\n  border-width: 2px;\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));\n  border-width: var(--mat-form-field-outlined-outline-width, 1px);\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));\n}\n.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {\n  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));\n}\n.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {\n  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);\n}\n\n.mdc-notched-outline__leading {\n  border-left: 1px solid;\n  border-right: none;\n  border-top-right-radius: 0;\n  border-bottom-right-radius: 0;\n  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n}\n.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {\n  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));\n}\n[dir=rtl] .mdc-notched-outline__leading {\n  border-left: none;\n  border-right: 1px solid;\n  border-bottom-left-radius: 0;\n  border-top-left-radius: 0;\n  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n}\n\n.mdc-notched-outline__trailing {\n  flex-grow: 1;\n  border-left: none;\n  border-right: 1px solid;\n  border-top-left-radius: 0;\n  border-bottom-left-radius: 0;\n  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n}\n[dir=rtl] .mdc-notched-outline__trailing {\n  border-left: 1px solid;\n  border-right: none;\n  border-top-right-radius: 0;\n  border-bottom-right-radius: 0;\n  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));\n}\n\n.mdc-notched-outline__notch {\n  flex: 0 0 auto;\n  width: auto;\n}\n.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {\n  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));\n}\n.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));\n}\n.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-top: 1px;\n}\n.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-top: 2px;\n}\n.mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-left: 0;\n  padding-right: 8px;\n  border-top: none;\n}\n[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {\n  padding-left: 8px;\n  padding-right: 0;\n}\n.mdc-notched-outline--no-label .mdc-notched-outline__notch {\n  display: none;\n}\n\n.mdc-line-ripple::before, .mdc-line-ripple::after {\n  position: absolute;\n  bottom: 0;\n  left: 0;\n  width: 100%;\n  border-bottom-style: solid;\n  content: \"\";\n}\n.mdc-line-ripple::before {\n  z-index: 1;\n  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {\n  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {\n  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));\n}\n.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {\n  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {\n  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {\n  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));\n}\n.mdc-line-ripple::after {\n  transform: scaleX(0);\n  opacity: 0;\n  z-index: 2;\n}\n.mdc-text-field--filled .mdc-line-ripple::after {\n  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);\n}\n.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {\n  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));\n}\n.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {\n  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));\n}\n\n.mdc-line-ripple--active::after {\n  transform: scaleX(1);\n  opacity: 1;\n}\n\n.mdc-line-ripple--deactivating::after {\n  opacity: 0;\n}\n\n.mdc-text-field--disabled {\n  pointer-events: none;\n}\n\n.mat-mdc-form-field-textarea-control {\n  vertical-align: middle;\n  resize: vertical;\n  box-sizing: border-box;\n  height: auto;\n  margin: 0;\n  padding: 0;\n  border: none;\n  overflow: auto;\n}\n\n.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font: inherit;\n  letter-spacing: inherit;\n  text-decoration: inherit;\n  text-transform: inherit;\n  border: none;\n}\n\n.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  line-height: normal;\n  pointer-events: all;\n  will-change: auto;\n}\n\n.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {\n  cursor: inherit;\n}\n\n.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {\n  height: auto;\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {\n  height: 23px;\n}\n\n.mat-mdc-text-field-wrapper {\n  height: auto;\n  flex: auto;\n  will-change: auto;\n}\n\n.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {\n  padding-left: 0;\n  --mat-mdc-form-field-label-offset-x: -16px;\n}\n\n.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {\n  padding-right: 0;\n}\n\n[dir=rtl] .mat-mdc-text-field-wrapper {\n  padding-left: 16px;\n  padding-right: 16px;\n}\n[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {\n  padding-left: 0;\n}\n[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {\n  padding-right: 0;\n}\n\n.mat-form-field-disabled .mdc-text-field__input::placeholder {\n  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {\n  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {\n  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {\n  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n\n.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n  opacity: 1;\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {\n  left: auto;\n  right: auto;\n}\n\n.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {\n  display: inline-block;\n}\n\n.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {\n  padding-top: 0;\n}\n\n.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {\n  border-left: 1px solid transparent;\n}\n\n[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {\n  border-left: none;\n  border-right: 1px solid transparent;\n}\n\n.mat-mdc-form-field-infix {\n  min-height: var(--mat-form-field-container-height, 56px);\n  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);\n  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);\n}\n.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {\n  padding-top: var(--mat-form-field-container-vertical-padding, 16px);\n  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);\n}\n\n.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {\n  top: calc(var(--mat-form-field-container-height, 56px) / 2);\n}\n\n.mdc-text-field--filled .mat-mdc-floating-label {\n  display: var(--mat-form-field-filled-label-display, block);\n}\n\n.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))\n    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));\n  transform: var(--mat-mdc-form-field-label-transform);\n}\n\n@keyframes _mat-form-field-subscript-animation {\n  from {\n    opacity: 0;\n    transform: translateY(-5px);\n  }\n  to {\n    opacity: 1;\n    transform: translateY(0);\n  }\n}\n.mat-mdc-form-field-subscript-wrapper {\n  box-sizing: border-box;\n  width: 100%;\n  position: relative;\n}\n\n.mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field-error-wrapper {\n  position: absolute;\n  top: 0;\n  left: 0;\n  right: 0;\n  padding: 0 16px;\n  opacity: 1;\n  transform: translateY(0);\n  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);\n}\n\n.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {\n  position: static;\n}\n\n.mat-mdc-form-field-bottom-align::before {\n  content: \"\";\n  display: inline-block;\n  height: 16px;\n}\n\n.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {\n  content: unset;\n}\n\n.mat-mdc-form-field-hint-end {\n  order: 1;\n}\n\n.mat-mdc-form-field-hint-wrapper {\n  display: flex;\n}\n\n.mat-mdc-form-field-hint-spacer {\n  flex: 1 0 1em;\n}\n\n.mat-mdc-form-field-error {\n  display: block;\n  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));\n}\n\n.mat-mdc-form-field-subscript-wrapper,\n.mat-mdc-form-field-bottom-align::before {\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));\n  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));\n  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));\n  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));\n  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));\n}\n\n.mat-mdc-form-field-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  opacity: 0;\n  pointer-events: none;\n  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));\n}\n.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {\n  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {\n  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);\n}\n\nselect.mat-mdc-form-field-input-control {\n  -moz-appearance: none;\n  -webkit-appearance: none;\n  background-color: transparent;\n  display: inline-flex;\n  box-sizing: border-box;\n}\nselect.mat-mdc-form-field-input-control:not(:disabled) {\n  cursor: pointer;\n}\nselect.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {\n  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));\n}\nselect.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {\n  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));\n}\n\n.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {\n  content: \"\";\n  width: 0;\n  height: 0;\n  border-left: 5px solid transparent;\n  border-right: 5px solid transparent;\n  border-top: 5px solid;\n  position: absolute;\n  right: 0;\n  top: 50%;\n  margin-top: -2.5px;\n  pointer-events: none;\n  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));\n}\n[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {\n  right: auto;\n  left: 0;\n}\n.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {\n  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));\n}\n.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {\n  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {\n  padding-right: 15px;\n}\n[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {\n  padding-right: 0;\n  padding-left: 15px;\n}\n\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {\n    outline: solid 1px;\n  }\n}\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {\n    outline-color: GrayText;\n  }\n}\n\n@media (forced-colors: active) {\n  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {\n    outline: dashed 3px;\n  }\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-form-field.mat-focused .mdc-notched-outline {\n    border: dashed 3px;\n  }\n}\n\n.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {\n  line-height: 1;\n}\n.mat-mdc-form-field-input-control::-webkit-datetime-edit {\n  line-height: 1;\n  padding: 0;\n  margin-bottom: -2px;\n}\n\n.mat-mdc-form-field {\n  --mat-mdc-form-field-floating-label-scale: 0.75;\n  display: inline-flex;\n  flex-direction: column;\n  min-width: 0;\n  text-align: left;\n  -moz-osx-font-smoothing: grayscale;\n  -webkit-font-smoothing: antialiased;\n  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));\n  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));\n  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));\n  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));\n  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));\n}\n.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {\n  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));\n}\n.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  font-size: var(--mat-form-field-outlined-label-text-populated-size);\n}\n[dir=rtl] .mat-mdc-form-field {\n  text-align: right;\n}\n\n.mat-mdc-form-field-flex {\n  display: inline-flex;\n  align-items: baseline;\n  box-sizing: border-box;\n  width: 100%;\n}\n\n.mat-mdc-text-field-wrapper {\n  width: 100%;\n  z-index: 0;\n}\n\n.mat-mdc-form-field-icon-prefix,\n.mat-mdc-form-field-icon-suffix {\n  align-self: center;\n  line-height: 0;\n  pointer-events: auto;\n  position: relative;\n  z-index: 1;\n}\n.mat-mdc-form-field-icon-prefix > .mat-icon,\n.mat-mdc-form-field-icon-suffix > .mat-icon {\n  padding: 0 12px;\n  box-sizing: content-box;\n}\n\n.mat-mdc-form-field-icon-prefix {\n  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {\n  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n\n.mat-mdc-form-field-icon-suffix {\n  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {\n  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {\n  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));\n}\n.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {\n  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));\n}\n.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {\n  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));\n}\n\n.mat-mdc-form-field-icon-prefix,\n[dir=rtl] .mat-mdc-form-field-icon-suffix {\n  padding: 0 4px 0 0;\n}\n\n.mat-mdc-form-field-icon-suffix,\n[dir=rtl] .mat-mdc-form-field-icon-prefix {\n  padding: 0 0 0 4px;\n}\n\n.mat-mdc-form-field-subscript-wrapper .mat-icon,\n.mat-mdc-form-field label .mat-icon {\n  width: 1em;\n  height: 1em;\n  font-size: inherit;\n}\n\n.mat-mdc-form-field-infix {\n  flex: auto;\n  min-width: 0;\n  width: 180px;\n  position: relative;\n  box-sizing: border-box;\n}\n.mat-mdc-form-field-infix:has(textarea[cols]) {\n  width: auto;\n}\n\n.mat-mdc-form-field .mdc-notched-outline__notch {\n  margin-left: -1px;\n  -webkit-clip-path: inset(-9em -999em -9em 1px);\n  clip-path: inset(-9em -999em -9em 1px);\n}\n[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {\n  margin-left: 0;\n  margin-right: -1px;\n  -webkit-clip-path: inset(-9em 1px -9em -999em);\n  clip-path: inset(-9em 1px -9em -999em);\n}\n\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {\n  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {\n  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {\n  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {\n  transition-delay: 40ms;\n  transition-duration: 110ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {\n  transition-duration: 75ms;\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {\n  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,\n.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {\n  animation-duration: 300ms;\n}\n\n.mdc-notched-outline .mdc-floating-label {\n  max-width: calc(100% + 1px);\n}\n\n.mdc-notched-outline--upgraded .mdc-floating-label--float-above {\n  max-width: calc(133.3333333333% + 1px);\n}\n"]
		}]
	}], () => [], {
		_textField: [{
			type: ViewChild,
			args: ["textField"]
		}],
		_iconPrefixContainer: [{
			type: ViewChild,
			args: ["iconPrefixContainer"]
		}],
		_textPrefixContainer: [{
			type: ViewChild,
			args: ["textPrefixContainer"]
		}],
		_iconSuffixContainer: [{
			type: ViewChild,
			args: ["iconSuffixContainer"]
		}],
		_textSuffixContainer: [{
			type: ViewChild,
			args: ["textSuffixContainer"]
		}],
		_floatingLabel: [{
			type: ViewChild,
			args: [MatFormFieldFloatingLabel]
		}],
		_notchedOutline: [{
			type: ViewChild,
			args: [MatFormFieldNotchedOutline]
		}],
		_lineRipple: [{
			type: ViewChild,
			args: [MatFormFieldLineRipple]
		}],
		_iconPrefixContainerSignal: [{
			type: ViewChild,
			args: ["iconPrefixContainer", { isSignal: true }]
		}],
		_textPrefixContainerSignal: [{
			type: ViewChild,
			args: ["textPrefixContainer", { isSignal: true }]
		}],
		_iconSuffixContainerSignal: [{
			type: ViewChild,
			args: ["iconSuffixContainer", { isSignal: true }]
		}],
		_textSuffixContainerSignal: [{
			type: ViewChild,
			args: ["textSuffixContainer", { isSignal: true }]
		}],
		_formFieldControl: [{
			type: ContentChild,
			args: [MatFormFieldControl]
		}],
		_prefixChildren: [{
			type: ContentChildren,
			args: [MAT_PREFIX, { descendants: true }]
		}],
		_suffixChildren: [{
			type: ContentChildren,
			args: [MAT_SUFFIX, { descendants: true }]
		}],
		_errorChildren: [{
			type: ContentChildren,
			args: [MAT_ERROR, { descendants: true }]
		}],
		_hintChildren: [{
			type: ContentChildren,
			args: [MatHint, { descendants: true }]
		}],
		_labelChild: [{
			type: ContentChild,
			args: [forwardRef(() => MatLabel), { isSignal: true }]
		}],
		hideRequiredMarker: [{ type: Input }],
		color: [{ type: Input }],
		floatLabel: [{ type: Input }],
		appearance: [{ type: Input }],
		subscriptSizing: [{ type: Input }],
		hintLabel: [{ type: Input }]
	});
})();
// node_modules/@angular/material/fesm2022/chips.mjs
var MAT_CHIPS_DEFAULT_OPTIONS = new InjectionToken("mat-chips-default-options", {
	providedIn: "root",
	factory: () => ({ separatorKeyCodes: [13] })
});
var MAT_CHIP_AVATAR = new InjectionToken("MatChipAvatar");
var MAT_CHIP_TRAILING_ICON = new InjectionToken("MatChipTrailingIcon");
var MAT_CHIP_EDIT = new InjectionToken("MatChipEdit");
var MAT_CHIP_REMOVE = new InjectionToken("MatChipRemove");
var MAT_CHIP = new InjectionToken("MatChip");
var MatChipContent = class MatChipContent {
	_elementRef = inject(ElementRef);
	_parentChip = inject(MAT_CHIP);
	_isPrimary = true;
	_isLeading = false;
	get disabled() {
		return this._disabled || this._parentChip?.disabled || false;
	}
	set disabled(value) {
		this._disabled = value;
	}
	_disabled = false;
	tabIndex = -1;
	_allowFocusWhenDisabled = false;
	_getDisabledAttribute() {
		return this.disabled && !this._allowFocusWhenDisabled ? "" : null;
	}
	constructor() {
		inject(_CdkPrivateStyleLoader).load(_StructuralStylesLoader);
		if (this._elementRef.nativeElement.nodeName === "BUTTON") this._elementRef.nativeElement.setAttribute("type", "button");
	}
	focus() {
		this._elementRef.nativeElement.focus();
	}
	static ɵfac = function MatChipContent_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipContent)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipContent,
		selectors: [[
			"",
			"matChipContent",
			""
		]],
		hostAttrs: [
			1,
			"mat-mdc-chip-action",
			"mdc-evolution-chip__action",
			"mdc-evolution-chip__action--presentational"
		],
		hostVars: 8,
		hostBindings: function MatChipContent_HostBindings(rf, ctx) {
			if (rf & 2) {
				ɵɵattribute("disabled", ctx._getDisabledAttribute())("aria-disabled", ctx.disabled);
				ɵɵclassProp("mdc-evolution-chip__action--primary", ctx._isPrimary)("mdc-evolution-chip__action--secondary", !ctx._isPrimary)("mdc-evolution-chip__action--trailing", !ctx._isPrimary && !ctx._isLeading);
			}
		},
		inputs: {
			disabled: [
				2,
				"disabled",
				"disabled",
				booleanAttribute
			],
			tabIndex: [
				2,
				"tabIndex",
				"tabIndex",
				(value) => value == null ? -1 : numberAttribute(value)
			],
			_allowFocusWhenDisabled: "_allowFocusWhenDisabled"
		}
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipContent, [{
		type: Directive,
		args: [{
			selector: "[matChipContent]",
			host: {
				"class": "mat-mdc-chip-action mdc-evolution-chip__action mdc-evolution-chip__action--presentational",
				"[class.mdc-evolution-chip__action--primary]": "_isPrimary",
				"[class.mdc-evolution-chip__action--secondary]": "!_isPrimary",
				"[class.mdc-evolution-chip__action--trailing]": "!_isPrimary && !_isLeading",
				"[attr.disabled]": "_getDisabledAttribute()",
				"[attr.aria-disabled]": "disabled"
			}
		}]
	}], () => [], {
		disabled: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		tabIndex: [{
			type: Input,
			args: [{ transform: (value) => value == null ? -1 : numberAttribute(value) }]
		}],
		_allowFocusWhenDisabled: [{ type: Input }]
	});
})();
var MatChipAction = class MatChipAction extends MatChipContent {
	_getTabindex() {
		return this.disabled && !this._allowFocusWhenDisabled ? null : this.tabIndex.toString();
	}
	_handleClick(event) {
		if (!this.disabled && this._isPrimary) {
			event.preventDefault();
			this._parentChip._handlePrimaryActionInteraction();
		}
	}
	_handleKeydown(event) {
		if ((event.keyCode === 13 || event.keyCode === 32) && !this.disabled && this._isPrimary && !this._parentChip._isEditing) {
			event.preventDefault();
			this._parentChip._handlePrimaryActionInteraction();
		}
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipAction_BaseFactory = void 0;
		return function MatChipAction_Factory(__ngFactoryType__) {
			return (ɵMatChipAction_BaseFactory || (ɵMatChipAction_BaseFactory = ɵɵgetInheritedFactory(MatChipAction)))(__ngFactoryType__ || MatChipAction);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipAction,
		selectors: [[
			"",
			"matChipAction",
			""
		]],
		hostVars: 3,
		hostBindings: function MatChipAction_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("click", function MatChipAction_click_HostBindingHandler($event) {
				return ctx._handleClick($event);
			})("keydown", function MatChipAction_keydown_HostBindingHandler($event) {
				return ctx._handleKeydown($event);
			});
			if (rf & 2) {
				ɵɵattribute("tabindex", ctx._getTabindex());
				ɵɵclassProp("mdc-evolution-chip__action--presentational", false);
			}
		},
		features: [ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipAction, [{
		type: Directive,
		args: [{
			selector: "[matChipAction]",
			host: {
				"[attr.tabindex]": "_getTabindex()",
				"[class.mdc-evolution-chip__action--presentational]": "false",
				"(click)": "_handleClick($event)",
				"(keydown)": "_handleKeydown($event)"
			}
		}]
	}], null, null);
})();
var MatChipAvatar = class MatChipAvatar {
	static ɵfac = function MatChipAvatar_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipAvatar)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipAvatar,
		selectors: [["mat-chip-avatar"], [
			"",
			"matChipAvatar",
			""
		]],
		hostAttrs: [
			"role",
			"img",
			1,
			"mat-mdc-chip-avatar",
			"mdc-evolution-chip__icon",
			"mdc-evolution-chip__icon--primary"
		],
		features: [ɵɵProvidersFeature([{
			provide: MAT_CHIP_AVATAR,
			useExisting: MatChipAvatar
		}])]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipAvatar, [{
		type: Directive,
		args: [{
			selector: "mat-chip-avatar, [matChipAvatar]",
			host: {
				"class": "mat-mdc-chip-avatar mdc-evolution-chip__icon mdc-evolution-chip__icon--primary",
				"role": "img"
			},
			providers: [{
				provide: MAT_CHIP_AVATAR,
				useExisting: MatChipAvatar
			}]
		}]
	}], null, null);
})();
var MatChipTrailingIcon = class MatChipTrailingIcon extends MatChipContent {
	_isPrimary = false;
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipTrailingIcon_BaseFactory = void 0;
		return function MatChipTrailingIcon_Factory(__ngFactoryType__) {
			return (ɵMatChipTrailingIcon_BaseFactory || (ɵMatChipTrailingIcon_BaseFactory = ɵɵgetInheritedFactory(MatChipTrailingIcon)))(__ngFactoryType__ || MatChipTrailingIcon);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipTrailingIcon,
		selectors: [["mat-chip-trailing-icon"], [
			"",
			"matChipTrailingIcon",
			""
		]],
		hostAttrs: [
			"aria-hidden",
			"true",
			1,
			"mat-mdc-chip-trailing-icon",
			"mdc-evolution-chip__icon",
			"mdc-evolution-chip__icon--trailing"
		],
		features: [ɵɵProvidersFeature([{
			provide: MAT_CHIP_TRAILING_ICON,
			useExisting: MatChipTrailingIcon
		}]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipTrailingIcon, [{
		type: Directive,
		args: [{
			selector: "mat-chip-trailing-icon, [matChipTrailingIcon]",
			host: {
				"class": "mat-mdc-chip-trailing-icon mdc-evolution-chip__icon mdc-evolution-chip__icon--trailing",
				"aria-hidden": "true"
			},
			providers: [{
				provide: MAT_CHIP_TRAILING_ICON,
				useExisting: MatChipTrailingIcon
			}]
		}]
	}], null, null);
})();
var MatChipEdit = class MatChipEdit extends MatChipAction {
	_isPrimary = false;
	_isLeading = true;
	_handleClick(event) {
		if (!this.disabled) {
			event.stopPropagation();
			event.preventDefault();
			this._parentChip._edit(event);
		}
	}
	_handleKeydown(event) {
		if ((event.keyCode === 13 || event.keyCode === 32) && !this.disabled) {
			event.stopPropagation();
			event.preventDefault();
			this._parentChip._edit(event);
		}
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipEdit_BaseFactory = void 0;
		return function MatChipEdit_Factory(__ngFactoryType__) {
			return (ɵMatChipEdit_BaseFactory || (ɵMatChipEdit_BaseFactory = ɵɵgetInheritedFactory(MatChipEdit)))(__ngFactoryType__ || MatChipEdit);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipEdit,
		selectors: [[
			"",
			"matChipEdit",
			""
		]],
		hostAttrs: [
			"role",
			"button",
			1,
			"mat-mdc-chip-edit",
			"mat-mdc-chip-avatar",
			"mat-focus-indicator",
			"mdc-evolution-chip__icon",
			"mdc-evolution-chip__icon--primary"
		],
		hostVars: 1,
		hostBindings: function MatChipEdit_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("aria-hidden", null);
		},
		features: [ɵɵProvidersFeature([{
			provide: MAT_CHIP_EDIT,
			useExisting: MatChipEdit
		}]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipEdit, [{
		type: Directive,
		args: [{
			selector: "[matChipEdit]",
			host: {
				"class": "mat-mdc-chip-edit mat-mdc-chip-avatar mat-focus-indicator mdc-evolution-chip__icon mdc-evolution-chip__icon--primary",
				"role": "button",
				"[attr.aria-hidden]": "null"
			},
			providers: [{
				provide: MAT_CHIP_EDIT,
				useExisting: MatChipEdit
			}]
		}]
	}], null, null);
})();
var MatChipRemove = class MatChipRemove extends MatChipAction {
	_isPrimary = false;
	_handleClick(event) {
		if (!this.disabled) {
			event.stopPropagation();
			event.preventDefault();
			this._parentChip.remove();
		}
	}
	_handleKeydown(event) {
		if ((event.keyCode === 13 || event.keyCode === 32) && !this.disabled) {
			event.stopPropagation();
			event.preventDefault();
			this._parentChip.remove();
		}
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipRemove_BaseFactory = void 0;
		return function MatChipRemove_Factory(__ngFactoryType__) {
			return (ɵMatChipRemove_BaseFactory || (ɵMatChipRemove_BaseFactory = ɵɵgetInheritedFactory(MatChipRemove)))(__ngFactoryType__ || MatChipRemove);
		};
	})();
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipRemove,
		selectors: [[
			"",
			"matChipRemove",
			""
		]],
		hostAttrs: [
			"role",
			"button",
			1,
			"mat-mdc-chip-remove",
			"mat-mdc-chip-trailing-icon",
			"mat-focus-indicator",
			"mdc-evolution-chip__icon",
			"mdc-evolution-chip__icon--trailing"
		],
		hostVars: 1,
		hostBindings: function MatChipRemove_HostBindings(rf, ctx) {
			if (rf & 2) ɵɵattribute("aria-hidden", null);
		},
		features: [ɵɵProvidersFeature([{
			provide: MAT_CHIP_REMOVE,
			useExisting: MatChipRemove
		}]), ɵɵInheritDefinitionFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipRemove, [{
		type: Directive,
		args: [{
			selector: "[matChipRemove]",
			host: {
				"class": "mat-mdc-chip-remove mat-mdc-chip-trailing-icon mat-focus-indicator mdc-evolution-chip__icon mdc-evolution-chip__icon--trailing",
				"role": "button",
				"[attr.aria-hidden]": "null"
			},
			providers: [{
				provide: MAT_CHIP_REMOVE,
				useExisting: MatChipRemove
			}]
		}]
	}], null, null);
})();
var MatChip = class MatChip {
	_changeDetectorRef = inject(ChangeDetectorRef);
	_elementRef = inject(ElementRef);
	_tagName = inject(HOST_TAG_NAME);
	_ngZone = inject(NgZone);
	_focusMonitor = inject(FocusMonitor);
	_globalRippleOptions = inject(MAT_RIPPLE_GLOBAL_OPTIONS, { optional: true });
	_document = inject(DOCUMENT);
	_onFocus = new Subject();
	_onBlur = new Subject();
	_isBasicChip = false;
	role = null;
	_hasFocusInternal = false;
	_pendingFocus = false;
	_actionChanges;
	_animationsDisabled = _animationsDisabled();
	_allLeadingIcons;
	_allTrailingIcons;
	_allEditIcons;
	_allRemoveIcons;
	_hasFocus() {
		return this._hasFocusInternal;
	}
	id = inject(_IdGenerator).getId("mat-mdc-chip-");
	ariaLabel = null;
	ariaDescription = null;
	_chipListDisabled = false;
	_hadFocusOnRemove = false;
	_textElement;
	get value() {
		return this._value !== void 0 ? this._value : this._textElement.textContent.trim();
	}
	set value(value) {
		this._value = value;
	}
	_value;
	color;
	removable = true;
	highlighted = false;
	disableRipple = false;
	get disabled() {
		return this._disabled || this._chipListDisabled;
	}
	set disabled(value) {
		this._disabled = value;
	}
	_disabled = false;
	removed = new EventEmitter();
	destroyed = new EventEmitter();
	basicChipAttrName = "mat-basic-chip";
	leadingIcon;
	editIcon;
	trailingIcon;
	removeIcon;
	primaryAction;
	_rippleLoader = inject(MatRippleLoader);
	_injector = inject(Injector);
	constructor() {
		const styleLoader = inject(_CdkPrivateStyleLoader);
		styleLoader.load(_StructuralStylesLoader);
		styleLoader.load(_VisuallyHiddenLoader);
		this._monitorFocus();
		this._rippleLoader?.configureRipple(this._elementRef.nativeElement, {
			className: "mat-mdc-chip-ripple",
			disabled: this._isRippleDisabled()
		});
	}
	ngOnInit() {
		this._isBasicChip = this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName) || this._tagName.toLowerCase() === this.basicChipAttrName;
	}
	ngAfterViewInit() {
		this._textElement = this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label");
		if (this._pendingFocus) {
			this._pendingFocus = false;
			this.focus();
		}
	}
	ngAfterContentInit() {
		this._actionChanges = merge(this._allLeadingIcons.changes, this._allTrailingIcons.changes, this._allEditIcons.changes, this._allRemoveIcons.changes).subscribe(() => this._changeDetectorRef.markForCheck());
	}
	ngDoCheck() {
		this._rippleLoader.setDisabled(this._elementRef.nativeElement, this._isRippleDisabled());
	}
	ngOnDestroy() {
		this.destroyed.emit({ chip: this });
		this.destroyed.complete();
		this._focusMonitor.stopMonitoring(this._elementRef);
		this._rippleLoader?.destroyRipple(this._elementRef.nativeElement);
		this._actionChanges?.unsubscribe();
	}
	remove() {
		if (this.removable) {
			this._hadFocusOnRemove = this._hasFocus();
			this.removed.emit({ chip: this });
		}
	}
	_isRippleDisabled() {
		return this.disabled || this.disableRipple || this._animationsDisabled || this._isBasicChip || !this._hasInteractiveActions() || !!this._globalRippleOptions?.disabled;
	}
	_hasTrailingIcon() {
		return !!(this.trailingIcon || this.removeIcon);
	}
	_handleKeydown(event) {
		if (event.keyCode === 8 && !event.repeat || event.keyCode === 46) {
			event.preventDefault();
			this.remove();
		}
	}
	focus() {
		if (!this.disabled) {
			if (this.primaryAction) this.primaryAction.focus();
			else this._pendingFocus = true;
		}
	}
	_getSourceAction(target) {
		return this._getActions().find((action) => {
			const element = action._elementRef.nativeElement;
			return element === target || element.contains(target);
		});
	}
	_getActions() {
		const result = [];
		if (this.editIcon) result.push(this.editIcon);
		if (this.primaryAction) result.push(this.primaryAction);
		if (this.removeIcon) result.push(this.removeIcon);
		return result;
	}
	_handlePrimaryActionInteraction() {}
	_hasInteractiveActions() {
		return this._getActions().length > 0;
	}
	_edit(event) {}
	_monitorFocus() {
		this._focusMonitor.monitor(this._elementRef, true).subscribe((origin) => {
			const hasFocus = origin !== null;
			if (hasFocus !== this._hasFocusInternal) {
				this._hasFocusInternal = hasFocus;
				if (hasFocus) this._onFocus.next({ chip: this });
				else {
					this._changeDetectorRef.markForCheck();
					setTimeout(() => this._ngZone.run(() => this._onBlur.next({ chip: this })));
				}
			}
		});
	}
	static ɵfac = function MatChip_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChip)();
	};
	static ɵcmp = (function() {
		const _c0 = [
			"*",
			[["mat-chip-avatar"], [
				"",
				"matChipAvatar",
				""
			]],
			[
				["mat-chip-trailing-icon"],
				[
					"",
					"matChipRemove",
					""
				],
				[
					"",
					"matChipTrailingIcon",
					""
				]
			]
		];
		const _c1 = [
			"*",
			"mat-chip-avatar, [matChipAvatar]",
			"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"
		];
		function MatChip_Conditional_3_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 3);
				ɵɵprojection(1, 1);
				ɵɵelementEnd();
			}
		}
		function MatChip_Conditional_8_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 7);
				ɵɵprojection(1, 2);
				ɵɵelementEnd();
			}
		}
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChip,
			selectors: [
				["mat-basic-chip"],
				[
					"",
					"mat-basic-chip",
					""
				],
				["mat-chip"],
				[
					"",
					"mat-chip",
					""
				]
			],
			contentQueries: function MatChip_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) ɵɵcontentQuery(dirIndex, MAT_CHIP_AVATAR, 5)(dirIndex, MAT_CHIP_EDIT, 5)(dirIndex, MAT_CHIP_TRAILING_ICON, 5)(dirIndex, MAT_CHIP_REMOVE, 5)(dirIndex, MAT_CHIP_AVATAR, 5)(dirIndex, MAT_CHIP_TRAILING_ICON, 5)(dirIndex, MAT_CHIP_EDIT, 5)(dirIndex, MAT_CHIP_REMOVE, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.leadingIcon = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.editIcon = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.trailingIcon = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.removeIcon = _t.first);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._allLeadingIcons = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._allTrailingIcons = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._allEditIcons = _t);
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._allRemoveIcons = _t);
				}
			},
			viewQuery: function MatChip_Query(rf, ctx) {
				if (rf & 1) ɵɵviewQuery(MatChipAction, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.primaryAction = _t.first);
				}
			},
			hostAttrs: [1, "mat-mdc-chip"],
			hostVars: 31,
			hostBindings: function MatChip_HostBindings(rf, ctx) {
				if (rf & 1) ɵɵlistener("keydown", function MatChip_keydown_HostBindingHandler($event) {
					return ctx._handleKeydown($event);
				});
				if (rf & 2) {
					ɵɵdomProperty("id", ctx.id);
					ɵɵattribute("role", ctx.role)("aria-label", ctx.ariaLabel);
					ɵɵclassMap("mat-" + (ctx.color || "primary"));
					ɵɵclassProp("mdc-evolution-chip", !ctx._isBasicChip)("mdc-evolution-chip--disabled", ctx.disabled)("mdc-evolution-chip--with-trailing-action", ctx._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic", ctx.leadingIcon)("mdc-evolution-chip--with-primary-icon", ctx.leadingIcon)("mdc-evolution-chip--with-avatar", ctx.leadingIcon)("mat-mdc-chip-with-avatar", ctx.leadingIcon)("mat-mdc-chip-highlighted", ctx.highlighted)("mat-mdc-chip-disabled", ctx.disabled)("mat-mdc-basic-chip", ctx._isBasicChip)("mat-mdc-standard-chip", !ctx._isBasicChip)("mat-mdc-chip-with-trailing-icon", ctx._hasTrailingIcon())("_mat-animation-noopable", ctx._animationsDisabled);
				}
			},
			inputs: {
				role: "role",
				id: "id",
				ariaLabel: [
					0,
					"aria-label",
					"ariaLabel"
				],
				ariaDescription: [
					0,
					"aria-description",
					"ariaDescription"
				],
				value: "value",
				color: "color",
				removable: [
					2,
					"removable",
					"removable",
					booleanAttribute
				],
				highlighted: [
					2,
					"highlighted",
					"highlighted",
					booleanAttribute
				],
				disableRipple: [
					2,
					"disableRipple",
					"disableRipple",
					booleanAttribute
				],
				disabled: [
					2,
					"disabled",
					"disabled",
					booleanAttribute
				]
			},
			outputs: {
				removed: "removed",
				destroyed: "destroyed"
			},
			exportAs: ["matChip"],
			features: [ɵɵProvidersFeature([{
				provide: MAT_CHIP,
				useExisting: MatChip
			}])],
			ngContentSelectors: _c1,
			decls: 9,
			vars: 2,
			consts: [
				[1, "mat-mdc-chip-focus-overlay"],
				[
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--primary"
				],
				["matChipContent", ""],
				[
					1,
					"mdc-evolution-chip__graphic",
					"mat-mdc-chip-graphic"
				],
				[
					1,
					"mdc-evolution-chip__text-label",
					"mat-mdc-chip-action-label"
				],
				[
					1,
					"mat-mdc-chip-primary-focus-indicator",
					"mat-focus-indicator"
				],
				[1, "mat-mdc-chip-touch-target"],
				[
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--trailing"
				]
			],
			template: function MatChip_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef(_c0);
					ɵɵelement(0, "span", 0);
					ɵɵelementStart(1, "span", 1)(2, "span", 2);
					ɵɵconditionalCreate(3, MatChip_Conditional_3_Template, 2, 0, "span", 3);
					ɵɵelementStart(4, "span", 4);
					ɵɵprojection(5);
					ɵɵelement(6, "span", 5);
					ɵɵelementEnd();
					ɵɵelement(7, "span", 6);
					ɵɵelementEnd()();
					ɵɵconditionalCreate(8, MatChip_Conditional_8_Template, 2, 0, "span", 7);
				}
				if (rf & 2) {
					ɵɵadvance(3);
					ɵɵconditional(ctx.leadingIcon ? 3 : -1);
					ɵɵadvance(5);
					ɵɵconditional(ctx._hasTrailingIcon() ? 8 : -1);
				}
			},
			dependencies: [MatChipContent],
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--%NS%mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-outline-width, 1px);\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));\n}\n.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));\n  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));\n  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));\n  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));\n  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  height: var(--%NS%mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChip, [{
		type: Component,
		args: [{
			selector: "mat-basic-chip, [mat-basic-chip], mat-chip, [mat-chip]",
			exportAs: "matChip",
			host: {
				"class": "mat-mdc-chip",
				"[class]": "\"mat-\" + (color || \"primary\")",
				"[class.mdc-evolution-chip]": "!_isBasicChip",
				"[class.mdc-evolution-chip--disabled]": "disabled",
				"[class.mdc-evolution-chip--with-trailing-action]": "_hasTrailingIcon()",
				"[class.mdc-evolution-chip--with-primary-graphic]": "leadingIcon",
				"[class.mdc-evolution-chip--with-primary-icon]": "leadingIcon",
				"[class.mdc-evolution-chip--with-avatar]": "leadingIcon",
				"[class.mat-mdc-chip-with-avatar]": "leadingIcon",
				"[class.mat-mdc-chip-highlighted]": "highlighted",
				"[class.mat-mdc-chip-disabled]": "disabled",
				"[class.mat-mdc-basic-chip]": "_isBasicChip",
				"[class.mat-mdc-standard-chip]": "!_isBasicChip",
				"[class.mat-mdc-chip-with-trailing-icon]": "_hasTrailingIcon()",
				"[class._mat-animation-noopable]": "_animationsDisabled",
				"[id]": "id",
				"[attr.role]": "role",
				"[attr.aria-label]": "ariaLabel",
				"(keydown)": "_handleKeydown($event)"
			},
			encapsulation: ViewEncapsulation.None,
			providers: [{
				provide: MAT_CHIP,
				useExisting: MatChip
			}],
			imports: [MatChipContent],
			template: "<span class=\"mat-mdc-chip-focus-overlay\"></span>\n\n<span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--primary\">\n  <span matChipContent>\n    @if (leadingIcon) {\n      <span class=\"mdc-evolution-chip__graphic mat-mdc-chip-graphic\">\n        <ng-content select=\"mat-chip-avatar, [matChipAvatar]\"></ng-content>\n      </span>\n    }\n    <span class=\"mdc-evolution-chip__text-label mat-mdc-chip-action-label\">\n      <ng-content></ng-content>\n      <span class=\"mat-mdc-chip-primary-focus-indicator mat-focus-indicator\"></span>\n    </span>\n    <span class=\"mat-mdc-chip-touch-target\"></span>\n  </span>\n</span>\n\n@if (_hasTrailingIcon()) {\n  <span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--trailing\">\n    <ng-content select=\"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]\"></ng-content>\n  </span>\n}\n",
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-outline-width, 1px);\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));\n}\n.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));\n  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));\n  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));\n  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));\n  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  height: var(--mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--mat-chip-with-icon-icon-size, 18px);\n  height: var(--mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"]
		}]
	}], () => [], {
		role: [{ type: Input }],
		_allLeadingIcons: [{
			type: ContentChildren,
			args: [MAT_CHIP_AVATAR, { descendants: true }]
		}],
		_allTrailingIcons: [{
			type: ContentChildren,
			args: [MAT_CHIP_TRAILING_ICON, { descendants: true }]
		}],
		_allEditIcons: [{
			type: ContentChildren,
			args: [MAT_CHIP_EDIT, { descendants: true }]
		}],
		_allRemoveIcons: [{
			type: ContentChildren,
			args: [MAT_CHIP_REMOVE, { descendants: true }]
		}],
		id: [{ type: Input }],
		ariaLabel: [{
			type: Input,
			args: ["aria-label"]
		}],
		ariaDescription: [{
			type: Input,
			args: ["aria-description"]
		}],
		value: [{ type: Input }],
		color: [{ type: Input }],
		removable: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		highlighted: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		disableRipple: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		disabled: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		removed: [{ type: Output }],
		destroyed: [{ type: Output }],
		leadingIcon: [{
			type: ContentChild,
			args: [MAT_CHIP_AVATAR]
		}],
		editIcon: [{
			type: ContentChild,
			args: [MAT_CHIP_EDIT]
		}],
		trailingIcon: [{
			type: ContentChild,
			args: [MAT_CHIP_TRAILING_ICON]
		}],
		removeIcon: [{
			type: ContentChild,
			args: [MAT_CHIP_REMOVE]
		}],
		primaryAction: [{
			type: ViewChild,
			args: [MatChipAction]
		}]
	});
})();
var MatChipSelectionChange = class {
	source;
	selected;
	isUserInput;
	constructor(source, selected, isUserInput = false) {
		this.source = source;
		this.selected = selected;
		this.isUserInput = isUserInput;
	}
};
var MatChipOption = class MatChipOption extends MatChip {
	_defaultOptions = inject(MAT_CHIPS_DEFAULT_OPTIONS, { optional: true });
	chipListSelectable = true;
	_chipListMultiple = false;
	_chipListHideSingleSelectionIndicator = this._defaultOptions?.hideSingleSelectionIndicator ?? false;
	get selectable() {
		return this._selectable && this.chipListSelectable;
	}
	set selectable(value) {
		this._selectable = value;
		this._changeDetectorRef.markForCheck();
	}
	_selectable = true;
	get selected() {
		return this._selected;
	}
	set selected(value) {
		this._setSelectedState(value, false, true);
	}
	_selected = false;
	get ariaSelected() {
		return this.selectable ? this.selected.toString() : null;
	}
	basicChipAttrName = "mat-basic-chip-option";
	selectionChange = new EventEmitter();
	ngOnInit() {
		super.ngOnInit();
		this.role = "presentation";
	}
	select() {
		this._setSelectedState(true, false, true);
	}
	deselect() {
		this._setSelectedState(false, false, true);
	}
	selectViaInteraction() {
		this._setSelectedState(true, true, true);
	}
	toggleSelected(isUserInput = false) {
		this._setSelectedState(!this.selected, isUserInput, true);
		return this.selected;
	}
	_handlePrimaryActionInteraction() {
		if (!this.disabled) {
			this.focus();
			if (this.selectable) this.toggleSelected(true);
		}
	}
	_hasLeadingGraphic() {
		if (this.leadingIcon) return true;
		return !this._chipListHideSingleSelectionIndicator || this._chipListMultiple;
	}
	_setSelectedState(isSelected, isUserInput, emitEvent) {
		if (isSelected !== this.selected) {
			this._selected = isSelected;
			if (emitEvent) this.selectionChange.emit({
				source: this,
				isUserInput,
				selected: this.selected
			});
			this._changeDetectorRef.markForCheck();
		}
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipOption_BaseFactory = void 0;
		return function MatChipOption_Factory(__ngFactoryType__) {
			return (ɵMatChipOption_BaseFactory || (ɵMatChipOption_BaseFactory = ɵɵgetInheritedFactory(MatChipOption)))(__ngFactoryType__ || MatChipOption);
		};
	})();
	static ɵcmp = (function() {
		const _c0 = [
			"*",
			[["mat-chip-avatar"], [
				"",
				"matChipAvatar",
				""
			]],
			[
				["mat-chip-trailing-icon"],
				[
					"",
					"matChipRemove",
					""
				],
				[
					"",
					"matChipTrailingIcon",
					""
				]
			]
		];
		const _c1 = [
			"*",
			"mat-chip-avatar, [matChipAvatar]",
			"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"
		];
		function MatChipOption_Conditional_3_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 3);
				ɵɵprojection(1, 1);
				ɵɵelementStart(2, "span", 8);
				ɵɵnamespaceSVG();
				ɵɵelementStart(3, "svg", 9);
				ɵɵelement(4, "path", 10);
				ɵɵelementEnd()()();
			}
		}
		function MatChipOption_Conditional_8_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 7);
				ɵɵprojection(1, 2);
				ɵɵelementEnd();
			}
		}
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChipOption,
			selectors: [
				["mat-basic-chip-option"],
				[
					"",
					"mat-basic-chip-option",
					""
				],
				["mat-chip-option"],
				[
					"",
					"mat-chip-option",
					""
				]
			],
			hostAttrs: [
				1,
				"mat-mdc-chip",
				"mat-mdc-chip-option"
			],
			hostVars: 37,
			hostBindings: function MatChipOption_HostBindings(rf, ctx) {
				if (rf & 2) {
					ɵɵdomProperty("id", ctx.id);
					ɵɵattribute("tabindex", null)("aria-label", null)("aria-description", null)("role", ctx.role);
					ɵɵclassProp("mdc-evolution-chip", !ctx._isBasicChip)("mdc-evolution-chip--filter", !ctx._isBasicChip)("mdc-evolution-chip--selectable", !ctx._isBasicChip)("mat-mdc-chip-selected", ctx.selected)("mat-mdc-chip-multiple", ctx._chipListMultiple)("mat-mdc-chip-disabled", ctx.disabled)("mat-mdc-chip-with-avatar", ctx.leadingIcon)("mdc-evolution-chip--disabled", ctx.disabled)("mdc-evolution-chip--selected", ctx.selected)("mdc-evolution-chip--selecting", !ctx._animationsDisabled)("mdc-evolution-chip--with-trailing-action", ctx._hasTrailingIcon())("mdc-evolution-chip--with-primary-icon", ctx.leadingIcon)("mdc-evolution-chip--with-primary-graphic", ctx._hasLeadingGraphic())("mdc-evolution-chip--with-avatar", ctx.leadingIcon)("mat-mdc-chip-highlighted", ctx.highlighted)("mat-mdc-chip-with-trailing-icon", ctx._hasTrailingIcon());
				}
			},
			inputs: {
				selectable: [
					2,
					"selectable",
					"selectable",
					booleanAttribute
				],
				selected: [
					2,
					"selected",
					"selected",
					booleanAttribute
				]
			},
			outputs: { selectionChange: "selectionChange" },
			features: [ɵɵProvidersFeature([{
				provide: MatChip,
				useExisting: MatChipOption
			}, {
				provide: MAT_CHIP,
				useExisting: MatChipOption
			}]), ɵɵInheritDefinitionFeature],
			ngContentSelectors: _c1,
			decls: 9,
			vars: 6,
			consts: [
				[1, "mat-mdc-chip-focus-overlay"],
				[
					"role",
					"presentation",
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--primary"
				],
				[
					"matChipAction",
					"",
					"role",
					"option",
					3,
					"_allowFocusWhenDisabled"
				],
				[
					1,
					"mdc-evolution-chip__graphic",
					"mat-mdc-chip-graphic"
				],
				[
					1,
					"mdc-evolution-chip__text-label",
					"mat-mdc-chip-action-label"
				],
				[
					1,
					"mat-mdc-chip-primary-focus-indicator",
					"mat-focus-indicator"
				],
				[1, "mat-mdc-chip-touch-target"],
				[
					"role",
					"presentation",
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--trailing"
				],
				[1, "mdc-evolution-chip__checkmark"],
				[
					"viewBox",
					"-2 -3 30 30",
					"focusable",
					"false",
					"aria-hidden",
					"true",
					1,
					"mdc-evolution-chip__checkmark-svg"
				],
				[
					"fill",
					"none",
					"stroke",
					"currentColor",
					"d",
					"M1.73,12.91 8.1,19.28 22.79,4.59",
					1,
					"mdc-evolution-chip__checkmark-path"
				]
			],
			template: function MatChipOption_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef(_c0);
					ɵɵelement(0, "span", 0);
					ɵɵelementStart(1, "span", 1)(2, "button", 2);
					ɵɵconditionalCreate(3, MatChipOption_Conditional_3_Template, 5, 0, "span", 3);
					ɵɵelementStart(4, "span", 4);
					ɵɵprojection(5);
					ɵɵelement(6, "span", 5)(7, "span", 6);
					ɵɵelementEnd()()();
					ɵɵconditionalCreate(8, MatChipOption_Conditional_8_Template, 2, 0, "span", 7);
				}
				if (rf & 2) {
					ɵɵadvance(2);
					ɵɵproperty("_allowFocusWhenDisabled", true);
					ɵɵattribute("aria-description", ctx.ariaDescription)("aria-label", ctx.ariaLabel)("aria-selected", ctx.ariaSelected);
					ɵɵadvance();
					ɵɵconditional(ctx._hasLeadingGraphic() ? 3 : -1);
					ɵɵadvance(5);
					ɵɵconditional(ctx._hasTrailingIcon() ? 8 : -1);
				}
			},
			dependencies: [MatChipAction],
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--%NS%mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-outline-width, 1px);\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));\n}\n.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));\n  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));\n  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));\n  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));\n  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  height: var(--%NS%mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipOption, [{
		type: Component,
		args: [{
			selector: "mat-basic-chip-option, [mat-basic-chip-option], mat-chip-option, [mat-chip-option]",
			host: {
				"class": "mat-mdc-chip mat-mdc-chip-option",
				"[class.mdc-evolution-chip]": "!_isBasicChip",
				"[class.mdc-evolution-chip--filter]": "!_isBasicChip",
				"[class.mdc-evolution-chip--selectable]": "!_isBasicChip",
				"[class.mat-mdc-chip-selected]": "selected",
				"[class.mat-mdc-chip-multiple]": "_chipListMultiple",
				"[class.mat-mdc-chip-disabled]": "disabled",
				"[class.mat-mdc-chip-with-avatar]": "leadingIcon",
				"[class.mdc-evolution-chip--disabled]": "disabled",
				"[class.mdc-evolution-chip--selected]": "selected",
				"[class.mdc-evolution-chip--selecting]": "!_animationsDisabled",
				"[class.mdc-evolution-chip--with-trailing-action]": "_hasTrailingIcon()",
				"[class.mdc-evolution-chip--with-primary-icon]": "leadingIcon",
				"[class.mdc-evolution-chip--with-primary-graphic]": "_hasLeadingGraphic()",
				"[class.mdc-evolution-chip--with-avatar]": "leadingIcon",
				"[class.mat-mdc-chip-highlighted]": "highlighted",
				"[class.mat-mdc-chip-with-trailing-icon]": "_hasTrailingIcon()",
				"[attr.tabindex]": "null",
				"[attr.aria-label]": "null",
				"[attr.aria-description]": "null",
				"[attr.role]": "role",
				"[id]": "id"
			},
			providers: [{
				provide: MatChip,
				useExisting: MatChipOption
			}, {
				provide: MAT_CHIP,
				useExisting: MatChipOption
			}],
			encapsulation: ViewEncapsulation.None,
			imports: [MatChipAction],
			template: "<span class=\"mat-mdc-chip-focus-overlay\"></span>\n\n<span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--primary\" role=\"presentation\">\n  <button\n    matChipAction\n    [_allowFocusWhenDisabled]=\"true\"\n    [attr.aria-description]=\"ariaDescription\"\n    [attr.aria-label]=\"ariaLabel\"\n    [attr.aria-selected]=\"ariaSelected\"\n    role=\"option\">\n    @if (_hasLeadingGraphic()) {\n      <span class=\"mdc-evolution-chip__graphic mat-mdc-chip-graphic\">\n        <ng-content select=\"mat-chip-avatar, [matChipAvatar]\"></ng-content>\n        <span class=\"mdc-evolution-chip__checkmark\">\n          <svg\n            class=\"mdc-evolution-chip__checkmark-svg\"\n            viewBox=\"-2 -3 30 30\"\n            focusable=\"false\"\n            aria-hidden=\"true\">\n            <path class=\"mdc-evolution-chip__checkmark-path\"\n                  fill=\"none\" stroke=\"currentColor\" d=\"M1.73,12.91 8.1,19.28 22.79,4.59\" />\n          </svg>\n        </span>\n      </span>\n    }\n    <span class=\"mdc-evolution-chip__text-label mat-mdc-chip-action-label\">\n      <ng-content></ng-content>\n      <span class=\"mat-mdc-chip-primary-focus-indicator mat-focus-indicator\"></span>\n      <span class=\"mat-mdc-chip-touch-target\"></span>\n    </span>\n  </button>\n</span>\n\n@if (_hasTrailingIcon()) {\n  <span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--trailing\" role=\"presentation\">\n    <ng-content select=\"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]\"></ng-content>\n  </span>\n}\n",
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-outline-width, 1px);\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));\n}\n.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));\n  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));\n  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));\n  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));\n  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  height: var(--mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--mat-chip-with-icon-icon-size, 18px);\n  height: var(--mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"]
		}]
	}], null, {
		selectable: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		selected: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		selectionChange: [{ type: Output }]
	});
})();
var MatChipEditInput = class MatChipEditInput {
	_elementRef = inject(ElementRef);
	_document = inject(DOCUMENT);
	initialize(initialValue) {
		this.getNativeElement().focus();
		this.setValue(initialValue);
	}
	getNativeElement() {
		return this._elementRef.nativeElement;
	}
	setValue(value) {
		this.getNativeElement().textContent = value;
		this._moveCursorToEndOfInput();
	}
	getValue() {
		return this.getNativeElement().textContent || "";
	}
	_moveCursorToEndOfInput() {
		const range = this._document.createRange();
		range.selectNodeContents(this.getNativeElement());
		range.collapse(false);
		const sel = window.getSelection();
		sel.removeAllRanges();
		sel.addRange(range);
	}
	static ɵfac = function MatChipEditInput_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipEditInput)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipEditInput,
		selectors: [[
			"span",
			"matChipEditInput",
			""
		]],
		hostAttrs: [
			"role",
			"textbox",
			"tabindex",
			"-1",
			"contenteditable",
			"true",
			1,
			"mat-chip-edit-input"
		]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipEditInput, [{
		type: Directive,
		args: [{
			selector: "span[matChipEditInput]",
			host: {
				"class": "mat-chip-edit-input",
				"role": "textbox",
				"tabindex": "-1",
				"contenteditable": "true"
			}
		}]
	}], null, null);
})();
var MatChipRow = class MatChipRow extends MatChip {
	basicChipAttrName = "mat-basic-chip-row";
	_renderer = inject(Renderer2);
	_cleanupMousedown;
	_editStartPending = false;
	editable = false;
	edited = new EventEmitter();
	defaultEditInput;
	contentEditInput;
	_alreadyFocused = false;
	_isEditing = false;
	constructor() {
		super();
		this.role = "row";
		this._onBlur.pipe(takeUntil(this.destroyed)).subscribe(() => {
			if (this._isEditing && !this._editStartPending) this._onEditFinish();
			this._alreadyFocused = false;
		});
	}
	ngAfterViewInit() {
		super.ngAfterViewInit();
		this._cleanupMousedown = this._ngZone.runOutsideAngular(() => this._renderer.listen(this._elementRef.nativeElement, "mousedown", () => {
			this._alreadyFocused = this._hasFocus();
		}));
	}
	ngOnDestroy() {
		super.ngOnDestroy();
		this._cleanupMousedown?.();
	}
	_hasLeadingActionIcon() {
		return !this._isEditing && !!this.editIcon;
	}
	_hasTrailingIcon() {
		return !this._isEditing && super._hasTrailingIcon();
	}
	_handleFocus() {
		if (!this._isEditing && !this.disabled) this.focus();
	}
	_handleKeydown(event) {
		if (event.keyCode === 13 && !this.disabled) {
			if (this._isEditing) {
				event.preventDefault();
				this._onEditFinish();
			} else if (this.editable) this._startEditing(event);
		} else if (this._isEditing) event.stopPropagation();
		else super._handleKeydown(event);
	}
	_handleClick(event) {
		if (!this.disabled && this.editable && !this._isEditing && this._alreadyFocused) {
			event.preventDefault();
			event.stopPropagation();
			this._startEditing(event);
		}
	}
	_handleDoubleclick(event) {
		if (!this.disabled && this.editable) this._startEditing(event);
	}
	_edit() {
		this._changeDetectorRef.markForCheck();
		this._startEditing();
	}
	_startEditing(event) {
		if (!this.primaryAction || this.removeIcon && !!event && this._getSourceAction(event.target) === this.removeIcon) return;
		const value = this.value;
		this._isEditing = this._editStartPending = true;
		afterNextRender(() => {
			this._getEditInput().initialize(value);
			setTimeout(() => this._ngZone.run(() => this._editStartPending = false));
		}, { injector: this._injector });
	}
	_onEditFinish() {
		this._isEditing = this._editStartPending = false;
		this.edited.emit({
			chip: this,
			value: this._getEditInput().getValue()
		});
		if (this._document.activeElement === this._getEditInput().getNativeElement() || this._document.activeElement === this._document.body) this.primaryAction.focus();
	}
	_isRippleDisabled() {
		return super._isRippleDisabled() || this._isEditing;
	}
	_getEditInput() {
		return this.contentEditInput || this.defaultEditInput;
	}
	static ɵfac = function MatChipRow_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipRow)();
	};
	static ɵcmp = (function() {
		const _c0 = [
			[[
				"",
				"matChipEdit",
				""
			]],
			[["mat-chip-avatar"], [
				"",
				"matChipAvatar",
				""
			]],
			[[
				"",
				"matChipEditInput",
				""
			]],
			"*",
			[
				["mat-chip-trailing-icon"],
				[
					"",
					"matChipRemove",
					""
				],
				[
					"",
					"matChipTrailingIcon",
					""
				]
			]
		];
		const _c1 = [
			"[matChipEdit]",
			"mat-chip-avatar, [matChipAvatar]",
			"[matChipEditInput]",
			"*",
			"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"
		];
		function MatChipRow_Conditional_0_Template(rf, ctx) {
			if (rf & 1) ɵɵelement(0, "span", 0);
		}
		function MatChipRow_Conditional_1_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 1);
				ɵɵprojection(1);
				ɵɵelementEnd();
			}
		}
		function MatChipRow_Conditional_3_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 3);
				ɵɵprojection(1, 1);
				ɵɵelementEnd();
			}
		}
		function MatChipRow_Conditional_5_Conditional_0_Template(rf, ctx) {
			if (rf & 1) ɵɵprojection(0, 2);
		}
		function MatChipRow_Conditional_5_Conditional_1_Template(rf, ctx) {
			if (rf & 1) ɵɵelement(0, "span", 8);
		}
		function MatChipRow_Conditional_5_Template(rf, ctx) {
			if (rf & 1) ɵɵconditionalCreate(0, MatChipRow_Conditional_5_Conditional_0_Template, 1, 0)(1, MatChipRow_Conditional_5_Conditional_1_Template, 1, 0, "span", 8);
			if (rf & 2) {
				const ctx_r0 = ɵɵnextContext();
				ɵɵconditional(ctx_r0.contentEditInput ? 0 : 1);
			}
		}
		function MatChipRow_Conditional_6_Template(rf, ctx) {
			if (rf & 1) ɵɵprojection(0, 3);
		}
		function MatChipRow_Conditional_9_Template(rf, ctx) {
			if (rf & 1) {
				ɵɵelementStart(0, "span", 7);
				ɵɵprojection(1, 4);
				ɵɵelementEnd();
			}
		}
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChipRow,
			selectors: [
				["mat-chip-row"],
				[
					"",
					"mat-chip-row",
					""
				],
				["mat-basic-chip-row"],
				[
					"",
					"mat-basic-chip-row",
					""
				]
			],
			contentQueries: function MatChipRow_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) ɵɵcontentQuery(dirIndex, MatChipEditInput, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.contentEditInput = _t.first);
				}
			},
			viewQuery: function MatChipRow_Query(rf, ctx) {
				if (rf & 1) ɵɵviewQuery(MatChipEditInput, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.defaultEditInput = _t.first);
				}
			},
			hostAttrs: [
				1,
				"mat-mdc-chip",
				"mat-mdc-chip-row",
				"mdc-evolution-chip"
			],
			hostVars: 29,
			hostBindings: function MatChipRow_HostBindings(rf, ctx) {
				if (rf & 1) ɵɵlistener("focus", function MatChipRow_focus_HostBindingHandler() {
					return ctx._handleFocus();
				})("click", function MatChipRow_click_HostBindingHandler($event) {
					return ctx._hasInteractiveActions() ? ctx._handleClick($event) : null;
				})("dblclick", function MatChipRow_dblclick_HostBindingHandler($event) {
					return ctx._handleDoubleclick($event);
				});
				if (rf & 2) {
					ɵɵdomProperty("id", ctx.id);
					ɵɵattribute("tabindex", ctx.disabled ? null : -1)("aria-label", null)("aria-description", null)("role", ctx.role);
					ɵɵclassProp("mat-mdc-chip-with-avatar", ctx.leadingIcon)("mat-mdc-chip-disabled", ctx.disabled)("mat-mdc-chip-editing", ctx._isEditing)("mat-mdc-chip-editable", ctx.editable)("mdc-evolution-chip--disabled", ctx.disabled)("mdc-evolution-chip--with-leading-action", ctx._hasLeadingActionIcon())("mdc-evolution-chip--with-trailing-action", ctx._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic", ctx.leadingIcon)("mdc-evolution-chip--with-primary-icon", ctx.leadingIcon)("mdc-evolution-chip--with-avatar", ctx.leadingIcon)("mat-mdc-chip-highlighted", ctx.highlighted)("mat-mdc-chip-with-trailing-icon", ctx._hasTrailingIcon());
				}
			},
			inputs: { editable: "editable" },
			outputs: { edited: "edited" },
			features: [ɵɵProvidersFeature([{
				provide: MatChip,
				useExisting: MatChipRow
			}, {
				provide: MAT_CHIP,
				useExisting: MatChipRow
			}]), ɵɵInheritDefinitionFeature],
			ngContentSelectors: _c1,
			decls: 10,
			vars: 8,
			consts: [
				[1, "mat-mdc-chip-focus-overlay"],
				[
					"role",
					"gridcell",
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--leading"
				],
				[
					"role",
					"gridcell",
					"matChipAction",
					"",
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--primary",
					3,
					"disabled"
				],
				[
					1,
					"mdc-evolution-chip__graphic",
					"mat-mdc-chip-graphic"
				],
				[
					1,
					"mdc-evolution-chip__text-label",
					"mat-mdc-chip-action-label"
				],
				[
					"aria-hidden",
					"true",
					1,
					"mat-mdc-chip-primary-focus-indicator",
					"mat-focus-indicator"
				],
				[1, "mat-mdc-chip-touch-target"],
				[
					"role",
					"gridcell",
					1,
					"mdc-evolution-chip__cell",
					"mdc-evolution-chip__cell--trailing"
				],
				["matChipEditInput", ""]
			],
			template: function MatChipRow_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef(_c0);
					ɵɵconditionalCreate(0, MatChipRow_Conditional_0_Template, 1, 0, "span", 0);
					ɵɵconditionalCreate(1, MatChipRow_Conditional_1_Template, 2, 0, "span", 1);
					ɵɵelementStart(2, "span", 2);
					ɵɵconditionalCreate(3, MatChipRow_Conditional_3_Template, 2, 0, "span", 3);
					ɵɵelementStart(4, "span", 4);
					ɵɵconditionalCreate(5, MatChipRow_Conditional_5_Template, 2, 1)(6, MatChipRow_Conditional_6_Template, 1, 0);
					ɵɵelement(7, "span", 5);
					ɵɵelementEnd();
					ɵɵelement(8, "span", 6);
					ɵɵelementEnd();
					ɵɵconditionalCreate(9, MatChipRow_Conditional_9_Template, 2, 0, "span", 7);
				}
				if (rf & 2) {
					ɵɵconditional(!ctx._isEditing ? 0 : -1);
					ɵɵadvance();
					ɵɵconditional(ctx._hasLeadingActionIcon() ? 1 : -1);
					ɵɵadvance();
					ɵɵproperty("disabled", ctx.disabled);
					ɵɵattribute("aria-description", ctx.ariaDescription)("aria-label", ctx.ariaLabel);
					ɵɵadvance();
					ɵɵconditional(ctx.leadingIcon ? 3 : -1);
					ɵɵadvance(2);
					ɵɵconditional(ctx._isEditing ? 5 : 6);
					ɵɵadvance(4);
					ɵɵconditional(ctx._hasTrailingIcon() ? 9 : -1);
				}
			},
			dependencies: [MatChipAction, MatChipEditInput],
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--%NS%mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-outline-width, 1px);\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));\n}\n.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {\n  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {\n  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));\n  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));\n  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));\n  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));\n  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);\n  height: var(--%NS%mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));\n  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));\n  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipRow, [{
		type: Component,
		args: [{
			selector: "mat-chip-row, [mat-chip-row], mat-basic-chip-row, [mat-basic-chip-row]",
			host: {
				"class": "mat-mdc-chip mat-mdc-chip-row mdc-evolution-chip",
				"[class.mat-mdc-chip-with-avatar]": "leadingIcon",
				"[class.mat-mdc-chip-disabled]": "disabled",
				"[class.mat-mdc-chip-editing]": "_isEditing",
				"[class.mat-mdc-chip-editable]": "editable",
				"[class.mdc-evolution-chip--disabled]": "disabled",
				"[class.mdc-evolution-chip--with-leading-action]": "_hasLeadingActionIcon()",
				"[class.mdc-evolution-chip--with-trailing-action]": "_hasTrailingIcon()",
				"[class.mdc-evolution-chip--with-primary-graphic]": "leadingIcon",
				"[class.mdc-evolution-chip--with-primary-icon]": "leadingIcon",
				"[class.mdc-evolution-chip--with-avatar]": "leadingIcon",
				"[class.mat-mdc-chip-highlighted]": "highlighted",
				"[class.mat-mdc-chip-with-trailing-icon]": "_hasTrailingIcon()",
				"[id]": "id",
				"[attr.tabindex]": "disabled ? null : -1",
				"[attr.aria-label]": "null",
				"[attr.aria-description]": "null",
				"[attr.role]": "role",
				"(focus)": "_handleFocus()",
				"(click)": "this._hasInteractiveActions() ? _handleClick($event) : null",
				"(dblclick)": "_handleDoubleclick($event)"
			},
			providers: [{
				provide: MatChip,
				useExisting: MatChipRow
			}, {
				provide: MAT_CHIP,
				useExisting: MatChipRow
			}],
			encapsulation: ViewEncapsulation.None,
			imports: [MatChipAction, MatChipEditInput],
			template: "@if (!_isEditing) {\n  <span class=\"mat-mdc-chip-focus-overlay\"></span>\n}\n\n@if (_hasLeadingActionIcon()) {\n  <span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--leading\" role=\"gridcell\">\n    <ng-content select=\"[matChipEdit]\"></ng-content>\n  </span>\n}\n<span class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--primary\" role=\"gridcell\"\n    matChipAction\n    [disabled]=\"disabled\"\n    [attr.aria-description]=\"ariaDescription\"\n    [attr.aria-label]=\"ariaLabel\">\n  @if (leadingIcon) {\n    <span class=\"mdc-evolution-chip__graphic mat-mdc-chip-graphic\">\n      <ng-content select=\"mat-chip-avatar, [matChipAvatar]\"></ng-content>\n    </span>\n  }\n\n  <span class=\"mdc-evolution-chip__text-label mat-mdc-chip-action-label\">\n    @if (_isEditing) {\n      @if (contentEditInput) {\n        <ng-content select=\"[matChipEditInput]\"></ng-content>\n      } @else {\n        <span matChipEditInput></span>\n      }\n    } @else {\n      <ng-content></ng-content>\n    }\n\n    <span class=\"mat-mdc-chip-primary-focus-indicator mat-focus-indicator\" aria-hidden=\"true\"></span>\n  </span>\n  <span class=\"mat-mdc-chip-touch-target\"></span>\n</span>\n\n@if (_hasTrailingIcon()) {\n  <span\n    class=\"mdc-evolution-chip__cell mdc-evolution-chip__cell--trailing\"\n    role=\"gridcell\">\n    <ng-content select=\"mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]\"></ng-content>\n  </span>\n}\n",
			styles: [".mdc-evolution-chip,\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  display: inline-flex;\n  align-items: center;\n}\n\n.mdc-evolution-chip {\n  position: relative;\n  max-width: 100%;\n}\n\n.mat-mdc-chip-touch-target {\n  position: absolute;\n  top: 50%;\n  left: 0;\n  right: 0;\n  height: 48px;\n  transform: translateY(-50%);\n  display: var(--mat-chip-touch-target-display, block);\n  pointer-events: auto;\n}\n\n.mdc-evolution-chip__cell,\n.mdc-evolution-chip__action {\n  height: 100%;\n}\n\n.mdc-evolution-chip__cell--primary {\n  flex-basis: 100%;\n  overflow-x: hidden;\n}\n\n.mdc-evolution-chip__cell--trailing {\n  flex: 1 0 auto;\n}\n\n.mdc-evolution-chip__action {\n  align-items: center;\n  background: none;\n  border: none;\n  box-sizing: content-box;\n  cursor: pointer;\n  display: inline-flex;\n  justify-content: center;\n  outline: none;\n  padding: 0;\n  text-decoration: none;\n  color: inherit;\n}\n\n.mdc-evolution-chip__action--presentational {\n  cursor: auto;\n}\n\n.mdc-evolution-chip--disabled,\n.mdc-evolution-chip__action:disabled {\n  pointer-events: none;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip--disabled,\n  .mdc-evolution-chip__action:disabled {\n    forced-color-adjust: none;\n  }\n}\n\n.mdc-evolution-chip__action--primary {\n  font: inherit;\n  letter-spacing: inherit;\n  white-space: inherit;\n  overflow-x: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-outline-width, 1px);\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  box-sizing: border-box;\n  content: \"\";\n  height: 100%;\n  left: 0;\n  position: absolute;\n  pointer-events: none;\n  top: 0;\n  width: 100%;\n  z-index: 1;\n  border-style: solid;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));\n}\n.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {\n  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {\n  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {\n  border-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {\n  font: inherit;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 12px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {\n  padding-left: 12px;\n  padding-right: 0;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {\n  padding-left: 0;\n  padding-right: 0;\n}\n\n.mdc-evolution-chip__action--secondary {\n  position: relative;\n  overflow: visible;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {\n  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {\n  padding-left: 8px;\n  padding-right: 8px;\n}\n\n.mdc-evolution-chip__text-label {\n  -webkit-user-select: none;\n  user-select: none;\n  white-space: nowrap;\n  text-overflow: ellipsis;\n  overflow: hidden;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__text-label {\n  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));\n  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));\n  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));\n  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));\n  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {\n  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));\n}\n\n.mdc-evolution-chip__graphic {\n  align-items: center;\n  display: inline-flex;\n  justify-content: center;\n  overflow: hidden;\n  pointer-events: none;\n  position: relative;\n  flex: 1 0 auto;\n}\n.mat-mdc-standard-chip .mdc-evolution-chip__graphic {\n  width: var(--mat-chip-with-avatar-avatar-size, 24px);\n  height: var(--mat-chip-with-avatar-avatar-size, 24px);\n  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {\n  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {\n  width: 0;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 6px;\n  padding-right: 6px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 4px;\n  padding-right: 8px;\n}\n[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {\n  padding-left: 8px;\n  padding-right: 4px;\n}\n.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {\n  padding-left: 0;\n}\n\n.mdc-evolution-chip__checkmark {\n  position: absolute;\n  opacity: 0;\n  top: 50%;\n  left: 50%;\n  height: 20px;\n  width: 20px;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {\n  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);\n  transform: translate(-75%, -50%);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  transform: translate(-50%, -50%);\n  opacity: 1;\n}\n\n.mdc-evolution-chip__checkmark-svg {\n  display: block;\n}\n\n.mdc-evolution-chip__checkmark-path {\n  stroke-width: 2px;\n  stroke-dasharray: 29.7833385;\n  stroke-dashoffset: 29.7833385;\n  stroke: currentColor;\n}\n.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {\n  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {\n  stroke-dashoffset: 0;\n}\n@media (forced-colors: active) {\n  .mdc-evolution-chip__checkmark-path {\n    stroke: CanvasText !important;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {\n  height: 18px;\n  width: 18px;\n  font-size: 18px;\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {\n  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {\n  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));\n}\n\n.mat-mdc-standard-chip {\n  border-radius: var(--mat-chip-container-shape-radius, 8px);\n  height: var(--mat-chip-container-height, 32px);\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-container-color, transparent);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-elevated-disabled-container-color);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {\n  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {\n  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));\n}\n@media (forced-colors: active) {\n  .mat-mdc-standard-chip {\n    outline: solid 1px;\n  }\n}\n\n.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {\n  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);\n  width: var(--mat-chip-with-icon-icon-size, 18px);\n  height: var(--mat-chip-with-icon-icon-size, 18px);\n  font-size: var(--mat-chip-with-icon-icon-size, 18px);\n}\n.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {\n  opacity: 0;\n}\n.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {\n  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-highlighted {\n  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));\n  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));\n  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);\n}\n\n.mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));\n  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {\n  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));\n  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));\n}\n\n.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {\n  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);\n}\n\n.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {\n  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);\n}\n\n.mat-mdc-standard-chip.mdc-evolution-chip--disabled {\n  opacity: var(--mat-chip-disabled-container-opacity, 1);\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));\n}\n.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {\n  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));\n}\n\n.mat-mdc-chip-edit, .mat-mdc-chip-remove {\n  opacity: var(--mat-chip-trailing-action-opacity, 1);\n}\n.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {\n  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));\n}\n.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n\n.mat-mdc-chip-selected .mat-mdc-chip-remove::after,\n.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {\n  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));\n}\n\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));\n}\n.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {\n  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));\n}\n\n.mat-mdc-standard-chip {\n  -webkit-tap-highlight-color: transparent;\n}\n.mat-mdc-standard-chip .mat-mdc-chip-graphic,\n.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {\n  box-sizing: content-box;\n}\n.mat-mdc-standard-chip._mat-animation-noopable,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,\n.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {\n  transition-duration: 1ms;\n  animation-duration: 1ms;\n}\n\n.mat-mdc-chip-focus-overlay {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  opacity: 0;\n  border-radius: inherit;\n  transition: opacity 150ms linear;\n}\n._mat-animation-noopable .mat-mdc-chip-focus-overlay {\n  transition: none;\n}\n.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {\n  display: none;\n}\n\n.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {\n  top: 0;\n  left: 0;\n  right: 0;\n  bottom: 0;\n  position: absolute;\n  pointer-events: none;\n  border-radius: inherit;\n}\n\n.mat-mdc-chip-avatar {\n  text-align: center;\n  line-height: 1;\n  color: var(--mat-chip-with-icon-icon-color, currentColor);\n}\n\n.mat-mdc-chip {\n  position: relative;\n  z-index: 0;\n}\n\n.mat-mdc-chip-action-label {\n  text-align: left;\n  z-index: 1;\n}\n[dir=rtl] .mat-mdc-chip-action-label {\n  text-align: right;\n}\n.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {\n  position: relative;\n}\n.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {\n  position: absolute;\n  top: 0;\n  right: 0;\n  bottom: 0;\n  left: 0;\n  pointer-events: none;\n}\n.mat-mdc-chip-action-label .mat-focus-indicator::before {\n  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);\n}\n\n.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {\n  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);\n  left: 8px;\n  right: 8px;\n}\n.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {\n  content: \"\";\n  display: block;\n  opacity: 0;\n  position: absolute;\n  top: -3px;\n  bottom: -3px;\n  left: 5px;\n  right: 5px;\n  border-radius: 50%;\n  box-sizing: border-box;\n  padding: 12px;\n  margin: -12px;\n  background-clip: content-box;\n}\n.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  width: 18px;\n  height: 18px;\n  font-size: 18px;\n  box-sizing: content-box;\n}\n\n.mat-chip-edit-input {\n  cursor: text;\n  display: inline-block;\n  color: inherit;\n  outline: 0;\n}\n\n@media (forced-colors: active) {\n  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {\n    outline-width: 3px;\n  }\n}\n\n.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {\n  content: \"\";\n}\n\n.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {\n  min-height: fit-content;\n}\n\nimg.mdc-evolution-chip__icon {\n  min-height: 0;\n}\n"]
		}]
	}], () => [], {
		editable: [{ type: Input }],
		edited: [{ type: Output }],
		defaultEditInput: [{
			type: ViewChild,
			args: [MatChipEditInput]
		}],
		contentEditInput: [{
			type: ContentChild,
			args: [MatChipEditInput]
		}]
	});
})();
var MatChipSet = class MatChipSet {
	_elementRef = inject(ElementRef);
	_changeDetectorRef = inject(ChangeDetectorRef);
	_dir = inject(Directionality, { optional: true });
	_lastDestroyedFocusedChipIndex = null;
	_keyManager;
	_destroyed = new Subject();
	_defaultRole = "presentation";
	get chipFocusChanges() {
		return this._getChipStream((chip) => chip._onFocus);
	}
	get chipDestroyedChanges() {
		return this._getChipStream((chip) => chip.destroyed);
	}
	get chipRemovedChanges() {
		return this._getChipStream((chip) => chip.removed);
	}
	get disabled() {
		return this._disabled;
	}
	set disabled(value) {
		this._disabled = value;
		this._syncChipsState();
	}
	_disabled = false;
	get empty() {
		return !this._chips || this._chips.length === 0;
	}
	get role() {
		if (this._explicitRole) return this._explicitRole;
		return this.empty ? null : this._defaultRole;
	}
	tabIndex = 0;
	set role(value) {
		this._explicitRole = value;
	}
	_explicitRole = null;
	get focused() {
		return this._hasFocusedChip();
	}
	_chips;
	_chipActions = new QueryList();
	ngAfterViewInit() {
		this._setUpFocusManagement();
		this._trackChipSetChanges();
		this._trackDestroyedFocusedChip();
	}
	ngOnDestroy() {
		this._keyManager?.destroy();
		this._chipActions.destroy();
		this._destroyed.next();
		this._destroyed.complete();
	}
	_hasFocusedChip() {
		return this._chips && this._chips.some((chip) => chip._hasFocus());
	}
	_syncChipsState() {
		this._chips?.forEach((chip) => {
			chip._chipListDisabled = this._disabled;
			chip._changeDetectorRef.markForCheck();
		});
	}
	focus() {}
	_handleKeydown(event) {
		if (this._originatesFromChip(event)) this._keyManager.onKeydown(event);
	}
	_isValidIndex(index) {
		return index >= 0 && index < this._chips.length;
	}
	_allowFocusEscape() {
		const previous = this._elementRef.nativeElement.tabIndex;
		if (previous !== -1) {
			this._elementRef.nativeElement.tabIndex = -1;
			setTimeout(() => this._elementRef.nativeElement.tabIndex = previous);
		}
	}
	_getChipStream(mappingFunction) {
		return this._chips.changes.pipe(startWith(null), switchMap(() => merge(...this._chips.map(mappingFunction))));
	}
	_originatesFromChip(event) {
		let currentElement = event.target;
		while (currentElement && currentElement !== this._elementRef.nativeElement) {
			if (currentElement.classList.contains("mat-mdc-chip")) return true;
			currentElement = currentElement.parentElement;
		}
		return false;
	}
	_setUpFocusManagement() {
		this._chips.changes.pipe(startWith(this._chips)).subscribe((chips) => {
			const actions = [];
			chips.forEach((chip) => chip._getActions().forEach((action) => actions.push(action)));
			this._chipActions.reset(actions);
			this._chipActions.notifyOnChanges();
		});
		this._keyManager = new FocusKeyManager(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir ? this._dir.value : "ltr").withHomeAndEnd().skipPredicate((action) => this._skipPredicate(action));
		this.chipFocusChanges.pipe(takeUntil(this._destroyed)).subscribe(({ chip }) => {
			const action = chip._getSourceAction(document.activeElement);
			if (action) this._keyManager.updateActiveItem(action);
		});
		this._dir?.change.pipe(takeUntil(this._destroyed)).subscribe((direction) => this._keyManager.withHorizontalOrientation(direction));
	}
	_skipPredicate(action) {
		return action.disabled;
	}
	_trackChipSetChanges() {
		this._chips.changes.pipe(startWith(null), takeUntil(this._destroyed)).subscribe(() => {
			if (this.disabled) Promise.resolve().then(() => this._syncChipsState());
			this._redirectDestroyedChipFocus();
		});
	}
	_trackDestroyedFocusedChip() {
		this.chipDestroyedChanges.pipe(takeUntil(this._destroyed)).subscribe((event) => {
			const chipIndex = this._chips.toArray().indexOf(event.chip);
			const hasFocus = event.chip._hasFocus();
			const wasLastFocused = event.chip._hadFocusOnRemove && this._keyManager.activeItem && event.chip._getActions().includes(this._keyManager.activeItem);
			const shouldMoveFocus = hasFocus || wasLastFocused;
			if (this._isValidIndex(chipIndex) && shouldMoveFocus) this._lastDestroyedFocusedChipIndex = chipIndex;
		});
	}
	_redirectDestroyedChipFocus() {
		if (this._lastDestroyedFocusedChipIndex == null) return;
		if (this._chips.length) {
			const newIndex = Math.min(this._lastDestroyedFocusedChipIndex, this._chips.length - 1);
			const chipToFocus = this._chips.toArray()[newIndex];
			if (chipToFocus.disabled) {
				if (this._chips.length === 1) this.focus();
				else this._keyManager.setPreviousItemActive();
			} else chipToFocus.focus();
		} else this.focus();
		this._lastDestroyedFocusedChipIndex = null;
	}
	static ɵfac = function MatChipSet_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipSet)();
	};
	static ɵcmp = (function() {
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChipSet,
			selectors: [["mat-chip-set"]],
			contentQueries: function MatChipSet_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) ɵɵcontentQuery(dirIndex, MatChip, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._chips = _t);
				}
			},
			hostAttrs: [
				1,
				"mat-mdc-chip-set",
				"mdc-evolution-chip-set"
			],
			hostVars: 1,
			hostBindings: function MatChipSet_HostBindings(rf, ctx) {
				if (rf & 1) ɵɵlistener("keydown", function MatChipSet_keydown_HostBindingHandler($event) {
					return ctx._handleKeydown($event);
				});
				if (rf & 2) ɵɵattribute("role", ctx.role);
			},
			inputs: {
				disabled: [
					2,
					"disabled",
					"disabled",
					booleanAttribute
				],
				role: "role",
				tabIndex: [
					2,
					"tabIndex",
					"tabIndex",
					(value) => value == null ? 0 : numberAttribute(value)
				]
			},
			ngContentSelectors: ["*"],
			decls: 2,
			vars: 0,
			consts: [[
				"role",
				"presentation",
				1,
				"mdc-evolution-chip-set__chips"
			]],
			template: function MatChipSet_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef();
					ɵɵdomElementStart(0, "div", 0);
					ɵɵprojection(1);
					ɵɵdomElementEnd();
				}
			},
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipSet, [{
		type: Component,
		args: [{
			selector: "mat-chip-set",
			template: `
    <div class="mdc-evolution-chip-set__chips" role="presentation">
      <ng-content></ng-content>
    </div>
  `,
			host: {
				"class": "mat-mdc-chip-set mdc-evolution-chip-set",
				"(keydown)": "_handleKeydown($event)",
				"[attr.role]": "role"
			},
			encapsulation: ViewEncapsulation.None,
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"]
		}]
	}], null, {
		disabled: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		role: [{ type: Input }],
		tabIndex: [{
			type: Input,
			args: [{ transform: (value) => value == null ? 0 : numberAttribute(value) }]
		}],
		_chips: [{
			type: ContentChildren,
			args: [MatChip, { descendants: true }]
		}]
	});
})();
var MatChipListboxChange = class {
	source;
	value;
	constructor(source, value) {
		this.source = source;
		this.value = value;
	}
};
var MAT_CHIP_LISTBOX_CONTROL_VALUE_ACCESSOR = {
	provide: NG_VALUE_ACCESSOR,
	useExisting: forwardRef(() => MatChipListbox),
	multi: true
};
var MatChipListbox = class MatChipListbox extends MatChipSet {
	_onTouched = () => {};
	_onChange = () => {};
	_defaultRole = "listbox";
	_defaultOptions = inject(MAT_CHIPS_DEFAULT_OPTIONS, { optional: true });
	get multiple() {
		return this._multiple;
	}
	set multiple(value) {
		this._multiple = value;
		this._syncListboxProperties();
	}
	_multiple = false;
	get selected() {
		const selectedChips = this._chips.toArray().filter((chip) => chip.selected);
		return this.multiple ? selectedChips : selectedChips[0];
	}
	ariaOrientation = "horizontal";
	get selectable() {
		return this._selectable;
	}
	set selectable(value) {
		this._selectable = value;
		this._syncListboxProperties();
	}
	_selectable = true;
	compareWith = (o1, o2) => o1 === o2;
	required = false;
	get hideSingleSelectionIndicator() {
		return this._hideSingleSelectionIndicator;
	}
	set hideSingleSelectionIndicator(value) {
		this._hideSingleSelectionIndicator = value;
		this._syncListboxProperties();
	}
	_hideSingleSelectionIndicator = this._defaultOptions?.hideSingleSelectionIndicator ?? false;
	get chipSelectionChanges() {
		return this._getChipStream((chip) => chip.selectionChange);
	}
	get chipBlurChanges() {
		return this._getChipStream((chip) => chip._onBlur);
	}
	get value() {
		return this._value;
	}
	set value(value) {
		if (this._chips && this._chips.length) this._setSelectionByValue(value, false);
		this._value = value;
	}
	_value;
	change = new EventEmitter();
	_chips = void 0;
	ngAfterContentInit() {
		this._chips.changes.pipe(startWith(null), takeUntil(this._destroyed)).subscribe(() => {
			if (this.value !== void 0) Promise.resolve().then(() => {
				this._setSelectionByValue(this.value, false);
			});
			this._syncListboxProperties();
		});
		this.chipBlurChanges.pipe(takeUntil(this._destroyed)).subscribe(() => this._blur());
		this.chipSelectionChanges.pipe(takeUntil(this._destroyed)).subscribe((event) => {
			if (!this.multiple) this._chips.forEach((chip) => {
				if (chip !== event.source) chip._setSelectedState(false, false, false);
			});
			if (event.isUserInput) this._propagateChanges();
		});
	}
	focus() {
		if (this.disabled) return;
		const firstSelectedChip = this._getFirstSelectedChip();
		if (firstSelectedChip && !firstSelectedChip.disabled) firstSelectedChip.focus();
		else if (this._chips.length > 0) this._keyManager.setFirstItemActive();
		else this._elementRef.nativeElement.focus();
	}
	writeValue(value) {
		if (value != null) this.value = value;
		else this.value = void 0;
	}
	registerOnChange(fn) {
		this._onChange = fn;
	}
	registerOnTouched(fn) {
		this._onTouched = fn;
	}
	setDisabledState(isDisabled) {
		this.disabled = isDisabled;
	}
	_setSelectionByValue(value, isUserInput = true) {
		this._clearSelection();
		if (Array.isArray(value)) value.forEach((currentValue) => this._selectValue(currentValue, isUserInput));
		else this._selectValue(value, isUserInput);
	}
	_blur() {
		if (!this.disabled) setTimeout(() => {
			if (!this.focused) this._markAsTouched();
		});
	}
	_keydown(event) {
		if (event.keyCode === 9) super._allowFocusEscape();
	}
	_markAsTouched() {
		this._onTouched();
		this._changeDetectorRef.markForCheck();
	}
	_propagateChanges() {
		let valueToEmit = null;
		if (Array.isArray(this.selected)) valueToEmit = this.selected.map((chip) => chip.value);
		else valueToEmit = this.selected ? this.selected.value : void 0;
		this._value = valueToEmit;
		this.change.emit(new MatChipListboxChange(this, valueToEmit));
		this._onChange(valueToEmit);
		this._changeDetectorRef.markForCheck();
	}
	_clearSelection(skip) {
		this._chips.forEach((chip) => {
			if (chip !== skip) chip.deselect();
		});
	}
	_selectValue(value, isUserInput) {
		const correspondingChip = this._chips.find((chip) => {
			return chip.value != null && this.compareWith(chip.value, value);
		});
		if (correspondingChip) isUserInput ? correspondingChip.selectViaInteraction() : correspondingChip.select();
		return correspondingChip;
	}
	_syncListboxProperties() {
		if (this._chips) Promise.resolve().then(() => {
			this._chips.forEach((chip) => {
				chip._chipListMultiple = this.multiple;
				chip.chipListSelectable = this._selectable;
				chip._chipListHideSingleSelectionIndicator = this.hideSingleSelectionIndicator;
				chip._changeDetectorRef.markForCheck();
			});
		});
	}
	_getFirstSelectedChip() {
		if (Array.isArray(this.selected)) return this.selected.length ? this.selected[0] : void 0;
		else return this.selected;
	}
	_skipPredicate(action) {
		return false;
	}
	static ɵfac = /*@__PURE__*/ (() => {
		let ɵMatChipListbox_BaseFactory = void 0;
		return function MatChipListbox_Factory(__ngFactoryType__) {
			return (ɵMatChipListbox_BaseFactory || (ɵMatChipListbox_BaseFactory = ɵɵgetInheritedFactory(MatChipListbox)))(__ngFactoryType__ || MatChipListbox);
		};
	})();
	static ɵcmp = (function() {
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChipListbox,
			selectors: [["mat-chip-listbox"]],
			contentQueries: function MatChipListbox_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) ɵɵcontentQuery(dirIndex, MatChipOption, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._chips = _t);
				}
			},
			hostAttrs: [
				1,
				"mdc-evolution-chip-set",
				"mat-mdc-chip-listbox"
			],
			hostVars: 10,
			hostBindings: function MatChipListbox_HostBindings(rf, ctx) {
				if (rf & 1) ɵɵlistener("focus", function MatChipListbox_focus_HostBindingHandler() {
					return ctx.focus();
				})("blur", function MatChipListbox_blur_HostBindingHandler() {
					return ctx._blur();
				})("keydown", function MatChipListbox_keydown_HostBindingHandler($event) {
					return ctx._keydown($event);
				});
				if (rf & 2) {
					ɵɵdomProperty("tabIndex", ctx.disabled || ctx.empty ? -1 : ctx.tabIndex);
					ɵɵattribute("role", ctx.role)("aria-required", ctx.role ? ctx.required : null)("aria-disabled", ctx.disabled.toString())("aria-multiselectable", ctx.multiple)("aria-orientation", ctx.ariaOrientation);
					ɵɵclassProp("mat-mdc-chip-list-disabled", ctx.disabled)("mat-mdc-chip-list-required", ctx.required);
				}
			},
			inputs: {
				multiple: [
					2,
					"multiple",
					"multiple",
					booleanAttribute
				],
				ariaOrientation: [
					0,
					"aria-orientation",
					"ariaOrientation"
				],
				selectable: [
					2,
					"selectable",
					"selectable",
					booleanAttribute
				],
				compareWith: "compareWith",
				required: [
					2,
					"required",
					"required",
					booleanAttribute
				],
				hideSingleSelectionIndicator: [
					2,
					"hideSingleSelectionIndicator",
					"hideSingleSelectionIndicator",
					booleanAttribute
				],
				value: "value"
			},
			outputs: { change: "change" },
			features: [ɵɵProvidersFeature([MAT_CHIP_LISTBOX_CONTROL_VALUE_ACCESSOR]), ɵɵInheritDefinitionFeature],
			ngContentSelectors: ["*"],
			decls: 2,
			vars: 0,
			consts: [[
				"role",
				"presentation",
				1,
				"mdc-evolution-chip-set__chips"
			]],
			template: function MatChipListbox_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef();
					ɵɵdomElementStart(0, "div", 0);
					ɵɵprojection(1);
					ɵɵdomElementEnd();
				}
			},
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipListbox, [{
		type: Component,
		args: [{
			selector: "mat-chip-listbox",
			template: `
    <div class="mdc-evolution-chip-set__chips" role="presentation">
      <ng-content></ng-content>
    </div>
  `,
			host: {
				"class": "mdc-evolution-chip-set mat-mdc-chip-listbox",
				"[attr.role]": "role",
				"[tabIndex]": "(disabled || empty) ? -1 : tabIndex",
				"[attr.aria-required]": "role ? required : null",
				"[attr.aria-disabled]": "disabled.toString()",
				"[attr.aria-multiselectable]": "multiple",
				"[attr.aria-orientation]": "ariaOrientation",
				"[class.mat-mdc-chip-list-disabled]": "disabled",
				"[class.mat-mdc-chip-list-required]": "required",
				"(focus)": "focus()",
				"(blur)": "_blur()",
				"(keydown)": "_keydown($event)"
			},
			providers: [MAT_CHIP_LISTBOX_CONTROL_VALUE_ACCESSOR],
			encapsulation: ViewEncapsulation.None,
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"]
		}]
	}], null, {
		multiple: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		ariaOrientation: [{
			type: Input,
			args: ["aria-orientation"]
		}],
		selectable: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		compareWith: [{ type: Input }],
		required: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		hideSingleSelectionIndicator: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		value: [{ type: Input }],
		change: [{ type: Output }],
		_chips: [{
			type: ContentChildren,
			args: [MatChipOption, { descendants: true }]
		}]
	});
})();
var MatChipGridChange = class {
	source;
	value;
	constructor(source, value) {
		this.source = source;
		this.value = value;
	}
};
var MatChipGrid = class MatChipGrid extends MatChipSet {
	ngControl = inject(NgControl, {
		optional: true,
		self: true
	});
	controlType = "mat-chip-grid";
	_chipInput;
	_defaultRole = "grid";
	_errorStateTracker;
	_uid = inject(_IdGenerator).getId("mat-chip-grid-");
	_ariaDescribedbyIds = [];
	_onTouched = () => {};
	_onChange = () => {};
	get disabled() {
		return this.ngControl ? !!this.ngControl.disabled : this._disabled;
	}
	set disabled(value) {
		this._disabled = value;
		this._syncChipsState();
		this.stateChanges.next();
	}
	get id() {
		return this._chipInput ? this._chipInput.id : this._uid;
	}
	get empty() {
		return (!this._chipInput || this._chipInput.empty) && (!this._chips || this._chips.length === 0);
	}
	get placeholder() {
		return this._chipInput ? this._chipInput.placeholder : this._placeholder;
	}
	set placeholder(value) {
		this._placeholder = value;
		this.stateChanges.next();
	}
	_placeholder = "";
	get focused() {
		return this._chipInput?.focused || this._hasFocusedChip();
	}
	get required() {
		return this._required ?? this.ngControl?.control?.hasValidator(Validators.required) ?? false;
	}
	set required(value) {
		this._required = value;
		this.stateChanges.next();
	}
	_required;
	get shouldLabelFloat() {
		return !this.empty || this.focused;
	}
	get value() {
		return this._value;
	}
	set value(value) {
		this._value = value;
	}
	_value = [];
	get errorStateMatcher() {
		return this._errorStateTracker.matcher;
	}
	set errorStateMatcher(value) {
		this._errorStateTracker.matcher = value;
	}
	get chipBlurChanges() {
		return this._getChipStream((chip) => chip._onBlur);
	}
	change = new EventEmitter();
	valueChange = new EventEmitter();
	_chips = void 0;
	stateChanges = new Subject();
	get errorState() {
		return this._errorStateTracker.errorState;
	}
	set errorState(value) {
		this._errorStateTracker.errorState = value;
	}
	constructor() {
		super();
		const parentForm = inject(NgForm, { optional: true });
		const parentFormGroup = inject(FormGroupDirective, { optional: true });
		const defaultErrorStateMatcher = inject(ErrorStateMatcher);
		const formField = inject(FORM_FIELD, {
			optional: true,
			self: true
		});
		if (this.ngControl) this.ngControl.valueAccessor = this;
		this._errorStateTracker = new _ErrorStateTracker(defaultErrorStateMatcher, formField || this.ngControl, parentFormGroup, parentForm, this.stateChanges);
	}
	ngAfterContentInit() {
		this.chipBlurChanges.pipe(takeUntil(this._destroyed)).subscribe(() => {
			this._blur();
			this.stateChanges.next();
		});
		merge(this.chipFocusChanges, this._chips.changes).pipe(takeUntil(this._destroyed)).subscribe(() => this.stateChanges.next());
	}
	ngDoCheck() {
		if (this.ngControl) this.updateErrorState();
	}
	ngOnDestroy() {
		super.ngOnDestroy();
		this.stateChanges.complete();
	}
	registerInput(inputElement) {
		this._chipInput = inputElement;
		this._chipInput.setDescribedByIds(this._ariaDescribedbyIds);
		this._elementRef.nativeElement.removeAttribute("aria-describedby");
	}
	onContainerClick(event) {
		if (!this.disabled && !this._originatesFromChip(event)) this.focus();
	}
	focus() {
		if (this.disabled || this._chipInput?.focused) return;
		if (!this._chips.length || this._chips.first.disabled) {
			if (!this._chipInput) return;
			Promise.resolve().then(() => this._chipInput.focus());
		} else {
			const activeItem = this._keyManager.activeItem;
			if (activeItem) activeItem.focus();
			else this._keyManager.setFirstItemActive();
		}
		this.stateChanges.next();
	}
	get describedByIds() {
		if (this._chipInput) return this._chipInput.describedByIds || [];
		const existing = this._elementRef.nativeElement.getAttribute("aria-describedby");
		return existing ? existing.split(" ") : [];
	}
	setDescribedByIds(ids) {
		this._ariaDescribedbyIds = ids;
		if (this._chipInput) this._chipInput.setDescribedByIds(ids);
		else if (ids.length) this._elementRef.nativeElement.setAttribute("aria-describedby", ids.join(" "));
		else this._elementRef.nativeElement.removeAttribute("aria-describedby");
	}
	writeValue(value) {
		this._value = value;
	}
	registerOnChange(fn) {
		this._onChange = fn;
	}
	registerOnTouched(fn) {
		this._onTouched = fn;
	}
	setDisabledState(isDisabled) {
		this.disabled = isDisabled;
		this.stateChanges.next();
	}
	updateErrorState() {
		this._errorStateTracker.updateErrorState();
	}
	_blur() {
		if (!this.disabled) setTimeout(() => {
			if (!this.focused) {
				this._propagateChanges();
				this._markAsTouched();
			}
		});
	}
	_allowFocusEscape() {
		if (!this._chipInput?.focused) super._allowFocusEscape();
	}
	_handleKeydown(event) {
		const keyCode = event.keyCode;
		const activeItem = this._keyManager.activeItem;
		if (keyCode === 9) {
			if (this._chipInput?.focused && hasModifierKey(event, "shiftKey") && this._chips.length && !this._chips.last.disabled) {
				event.preventDefault();
				if (activeItem) this._keyManager.setActiveItem(activeItem);
				else this._focusLastChip();
			} else super._allowFocusEscape();
		} else if (!this._chipInput?.focused) {
			if ((keyCode === 38 || keyCode === 40) && activeItem) {
				const eligibleActions = this._chipActions.filter((action) => action._isPrimary === activeItem._isPrimary && !this._skipPredicate(action));
				const currentIndex = eligibleActions.indexOf(activeItem);
				const delta = event.keyCode === 38 ? -1 : 1;
				event.preventDefault();
				if (currentIndex > -1 && this._isValidIndex(currentIndex + delta)) this._keyManager.setActiveItem(eligibleActions[currentIndex + delta]);
			} else super._handleKeydown(event);
		}
		this.stateChanges.next();
	}
	_redirectDestroyedChipFocus() {
		if (this._lastDestroyedFocusedChipIndex === null) return;
		super._redirectDestroyedChipFocus();
		if (!this._chips.length || this._chips.length === 1 && this._chips.first.disabled) this._keyManager.updateActiveItem(-1);
	}
	_focusLastChip() {
		if (this._chips.length) this._chips.last.focus();
	}
	_propagateChanges() {
		const valueToEmit = this._chips.length ? this._chips.toArray().map((chip) => chip.value) : [];
		this._value = valueToEmit;
		this.change.emit(new MatChipGridChange(this, valueToEmit));
		this.valueChange.emit(valueToEmit);
		this._onChange(valueToEmit);
		this._changeDetectorRef.markForCheck();
	}
	_markAsTouched() {
		this._onTouched();
		this._changeDetectorRef.markForCheck();
		this.stateChanges.next();
	}
	static ɵfac = function MatChipGrid_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipGrid)();
	};
	static ɵcmp = (function() {
		return /*@__PURE__*/ ɵɵdefineComponent({
			type: MatChipGrid,
			selectors: [["mat-chip-grid"]],
			contentQueries: function MatChipGrid_ContentQueries(rf, ctx, dirIndex) {
				if (rf & 1) ɵɵcontentQuery(dirIndex, MatChipRow, 5);
				if (rf & 2) {
					let _t = void 0;
					ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx._chips = _t);
				}
			},
			hostAttrs: [
				1,
				"mat-mdc-chip-set",
				"mat-mdc-chip-grid",
				"mdc-evolution-chip-set"
			],
			hostVars: 10,
			hostBindings: function MatChipGrid_HostBindings(rf, ctx) {
				if (rf & 1) ɵɵlistener("focus", function MatChipGrid_focus_HostBindingHandler() {
					return ctx.focus();
				})("blur", function MatChipGrid_blur_HostBindingHandler() {
					return ctx._blur();
				});
				if (rf & 2) {
					ɵɵattribute("role", ctx.role)("tabindex", ctx.disabled || ctx._chips && ctx._chips.length === 0 ? -1 : ctx.tabIndex)("aria-disabled", ctx.disabled.toString())("aria-invalid", ctx.errorState);
					ɵɵclassProp("mat-mdc-chip-list-disabled", ctx.disabled)("mat-mdc-chip-list-invalid", ctx.errorState)("mat-mdc-chip-list-required", ctx.required);
				}
			},
			inputs: {
				disabled: [
					2,
					"disabled",
					"disabled",
					booleanAttribute
				],
				placeholder: "placeholder",
				required: [
					2,
					"required",
					"required",
					booleanAttribute
				],
				value: "value",
				errorStateMatcher: "errorStateMatcher"
			},
			outputs: {
				change: "change",
				valueChange: "valueChange"
			},
			features: [ɵɵProvidersFeature([{
				provide: MatFormFieldControl,
				useExisting: MatChipGrid
			}]), ɵɵInheritDefinitionFeature],
			ngContentSelectors: ["*"],
			decls: 2,
			vars: 0,
			consts: [[
				"role",
				"presentation",
				1,
				"mdc-evolution-chip-set__chips"
			]],
			template: function MatChipGrid_Template(rf, ctx) {
				if (rf & 1) {
					ɵɵprojectionDef();
					ɵɵdomElementStart(0, "div", 0);
					ɵɵprojection(1);
					ɵɵdomElementEnd();
				}
			},
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"],
			encapsulation: 2
		});
	})();
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipGrid, [{
		type: Component,
		args: [{
			selector: "mat-chip-grid",
			template: `
    <div class="mdc-evolution-chip-set__chips" role="presentation">
      <ng-content></ng-content>
    </div>
  `,
			host: {
				"class": "mat-mdc-chip-set mat-mdc-chip-grid mdc-evolution-chip-set",
				"[attr.role]": "role",
				"[attr.tabindex]": "(disabled || (_chips && _chips.length === 0)) ? -1 : tabIndex",
				"[attr.aria-disabled]": "disabled.toString()",
				"[attr.aria-invalid]": "errorState",
				"[class.mat-mdc-chip-list-disabled]": "disabled",
				"[class.mat-mdc-chip-list-invalid]": "errorState",
				"[class.mat-mdc-chip-list-required]": "required",
				"(focus)": "focus()",
				"(blur)": "_blur()"
			},
			providers: [{
				provide: MatFormFieldControl,
				useExisting: MatChipGrid
			}],
			encapsulation: ViewEncapsulation.None,
			styles: [".mat-mdc-chip-set {\n  display: flex;\n}\n.mat-mdc-chip-set:focus {\n  outline: none;\n}\n.mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  min-width: 100%;\n  margin-left: -8px;\n  margin-right: 0;\n}\n.mat-mdc-chip-set .mdc-evolution-chip {\n  margin: 8px 0 8px 8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {\n  margin-left: 0;\n  margin-right: -8px;\n}\n[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {\n  margin-left: 0;\n  margin-right: 8px;\n}\n\n.mdc-evolution-chip-set__chips {\n  display: flex;\n  flex-flow: wrap;\n  min-width: 0;\n}\n\n.mat-mdc-chip-set-stacked {\n  flex-direction: column;\n  align-items: flex-start;\n}\n.mat-mdc-chip-set-stacked .mat-mdc-chip {\n  width: 100%;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {\n  flex-grow: 0;\n}\n.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {\n  flex-basis: 100%;\n  justify-content: start;\n}\n\ninput.mat-mdc-chip-input {\n  flex: 1 0 150px;\n  margin-left: 8px;\n}\n[dir=rtl] input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 8px;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {\n  opacity: 1;\n}\n.mat-mdc-chip-set + input.mat-mdc-chip-input {\n  margin-left: 0;\n  margin-right: 0;\n}\n"]
		}]
	}], () => [], {
		disabled: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		placeholder: [{ type: Input }],
		required: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		value: [{ type: Input }],
		errorStateMatcher: [{ type: Input }],
		change: [{ type: Output }],
		valueChange: [{ type: Output }],
		_chips: [{
			type: ContentChildren,
			args: [MatChipRow, { descendants: true }]
		}]
	});
})();
var MatChipInput = class MatChipInput {
	_elementRef = inject(ElementRef);
	focused = false;
	get chipGrid() {
		return this._chipGrid;
	}
	set chipGrid(value) {
		if (value) {
			this._chipGrid = value;
			this._chipGrid.registerInput(this);
		}
	}
	_chipGrid;
	addOnBlur = false;
	separatorKeyCodes;
	chipEnd = new EventEmitter();
	placeholder = "";
	id = inject(_IdGenerator).getId("mat-mdc-chip-list-input-");
	get disabled() {
		return this._disabled || this._chipGrid && this._chipGrid.disabled;
	}
	set disabled(value) {
		this._disabled = value;
	}
	_disabled = false;
	readonly = false;
	disabledInteractive;
	get empty() {
		return !this.inputElement.value;
	}
	inputElement;
	constructor() {
		const defaultOptions = inject(MAT_CHIPS_DEFAULT_OPTIONS);
		const formField = inject(MAT_FORM_FIELD, { optional: true });
		this.inputElement = this._elementRef.nativeElement;
		this.separatorKeyCodes = defaultOptions.separatorKeyCodes;
		this.disabledInteractive = defaultOptions.inputDisabledInteractive ?? false;
		if (formField) this.inputElement.classList.add("mat-mdc-form-field-input-control");
	}
	ngOnChanges() {
		this._chipGrid.stateChanges.next();
	}
	ngOnDestroy() {
		this.chipEnd.complete();
	}
	_keydown(event) {
		if (this.empty && event.keyCode === 8) {
			if (!event.repeat) this._chipGrid._focusLastChip();
			event.preventDefault();
		} else this._emitChipEnd(event);
	}
	_blur() {
		if (this.addOnBlur) this._emitChipEnd();
		this.focused = false;
		if (!this._chipGrid.focused) this._chipGrid._blur();
		this._chipGrid.stateChanges.next();
	}
	_focus() {
		this.focused = true;
		this._chipGrid.stateChanges.next();
	}
	_emitChipEnd(event) {
		if (!event || this._isSeparatorKey(event) && !event.repeat) {
			this.chipEnd.emit({
				input: this.inputElement,
				value: this.inputElement.value,
				chipInput: this
			});
			event?.preventDefault();
		}
	}
	_onInput() {
		this._chipGrid.stateChanges.next();
	}
	focus() {
		this.inputElement.focus();
	}
	clear() {
		this.inputElement.value = "";
	}
	get describedByIds() {
		return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ") || [];
	}
	setDescribedByIds(ids) {
		const element = this._elementRef.nativeElement;
		if (ids.length) element.setAttribute("aria-describedby", ids.join(" "));
		else element.removeAttribute("aria-describedby");
	}
	_isSeparatorKey(event) {
		if (!this.separatorKeyCodes) return false;
		for (const key of this.separatorKeyCodes) {
			let keyCode;
			let modifiers;
			if (typeof key === "number") {
				keyCode = key;
				modifiers = null;
			} else {
				keyCode = key.keyCode;
				modifiers = key.modifiers;
			}
			const modifiersMatch = !modifiers?.length ? !hasModifierKey(event) : hasModifierKey(event, ...modifiers);
			if (keyCode === event.keyCode && modifiersMatch) return true;
		}
		return false;
	}
	_getReadonlyAttribute() {
		return this.readonly || this.disabled && this.disabledInteractive ? "true" : null;
	}
	static ɵfac = function MatChipInput_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipInput)();
	};
	static ɵdir = /*@__PURE__*/ ɵɵdefineDirective({
		type: MatChipInput,
		selectors: [[
			"input",
			"matChipInputFor",
			""
		]],
		hostAttrs: [
			1,
			"mat-mdc-chip-input",
			"mat-mdc-input-element",
			"mdc-text-field__input",
			"mat-input-element"
		],
		hostVars: 8,
		hostBindings: function MatChipInput_HostBindings(rf, ctx) {
			if (rf & 1) ɵɵlistener("keydown", function MatChipInput_keydown_HostBindingHandler($event) {
				return ctx._keydown($event);
			})("blur", function MatChipInput_blur_HostBindingHandler() {
				return ctx._blur();
			})("focus", function MatChipInput_focus_HostBindingHandler() {
				return ctx._focus();
			})("input", function MatChipInput_input_HostBindingHandler() {
				return ctx._onInput();
			});
			if (rf & 2) {
				ɵɵdomProperty("id", ctx.id);
				ɵɵattribute("disabled", ctx.disabled && !ctx.disabledInteractive ? "" : null)("placeholder", ctx.placeholder || null)("aria-invalid", ctx._chipGrid && ctx._chipGrid.ngControl ? ctx._chipGrid.ngControl.invalid : null)("aria-required", ctx._chipGrid && ctx._chipGrid.required || null)("aria-disabled", ctx.disabled && ctx.disabledInteractive ? "true" : null)("readonly", ctx._getReadonlyAttribute())("required", ctx._chipGrid && ctx._chipGrid.required || null);
			}
		},
		inputs: {
			chipGrid: [
				0,
				"matChipInputFor",
				"chipGrid"
			],
			addOnBlur: [
				2,
				"matChipInputAddOnBlur",
				"addOnBlur",
				booleanAttribute
			],
			separatorKeyCodes: [
				0,
				"matChipInputSeparatorKeyCodes",
				"separatorKeyCodes"
			],
			placeholder: "placeholder",
			id: "id",
			disabled: [
				2,
				"disabled",
				"disabled",
				booleanAttribute
			],
			readonly: [
				2,
				"readonly",
				"readonly",
				booleanAttribute
			],
			disabledInteractive: [
				2,
				"matChipInputDisabledInteractive",
				"disabledInteractive",
				booleanAttribute
			]
		},
		outputs: { chipEnd: "matChipInputTokenEnd" },
		exportAs: ["matChipInput", "matChipInputFor"],
		features: [ɵɵNgOnChangesFeature]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipInput, [{
		type: Directive,
		args: [{
			selector: "input[matChipInputFor]",
			exportAs: "matChipInput, matChipInputFor",
			host: {
				"class": "mat-mdc-chip-input mat-mdc-input-element mdc-text-field__input mat-input-element",
				"(keydown)": "_keydown($event)",
				"(blur)": "_blur()",
				"(focus)": "_focus()",
				"(input)": "_onInput()",
				"[id]": "id",
				"[attr.disabled]": "disabled && !disabledInteractive ? \"\" : null",
				"[attr.placeholder]": "placeholder || null",
				"[attr.aria-invalid]": "_chipGrid && _chipGrid.ngControl ? _chipGrid.ngControl.invalid : null",
				"[attr.aria-required]": "_chipGrid && _chipGrid.required || null",
				"[attr.aria-disabled]": "disabled && disabledInteractive ? \"true\" : null",
				"[attr.readonly]": "_getReadonlyAttribute()",
				"[attr.required]": "_chipGrid && _chipGrid.required || null"
			}
		}]
	}], () => [], {
		chipGrid: [{
			type: Input,
			args: ["matChipInputFor"]
		}],
		addOnBlur: [{
			type: Input,
			args: [{
				alias: "matChipInputAddOnBlur",
				transform: booleanAttribute
			}]
		}],
		separatorKeyCodes: [{
			type: Input,
			args: ["matChipInputSeparatorKeyCodes"]
		}],
		chipEnd: [{
			type: Output,
			args: ["matChipInputTokenEnd"]
		}],
		placeholder: [{ type: Input }],
		id: [{ type: Input }],
		disabled: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		readonly: [{
			type: Input,
			args: [{ transform: booleanAttribute }]
		}],
		disabledInteractive: [{
			type: Input,
			args: [{
				alias: "matChipInputDisabledInteractive",
				transform: booleanAttribute
			}]
		}]
	});
})();
var CHIP_DECLARATIONS = [
	MatChip,
	MatChipAvatar,
	MatChipEdit,
	MatChipEditInput,
	MatChipGrid,
	MatChipInput,
	MatChipListbox,
	MatChipOption,
	MatChipRemove,
	MatChipRow,
	MatChipSet,
	MatChipTrailingIcon
];
var MatChipsModule = class MatChipsModule {
	static ɵfac = function MatChipsModule_Factory(__ngFactoryType__) {
		return new (__ngFactoryType__ || MatChipsModule)();
	};
	static ɵmod = /*@__PURE__*/ ɵɵdefineNgModule({
		type: MatChipsModule,
		imports: [
			MatRippleModule,
			MatChipAction,
			MatChip,
			MatChipAvatar,
			MatChipEdit,
			MatChipEditInput,
			MatChipGrid,
			MatChipInput,
			MatChipListbox,
			MatChipOption,
			MatChipRemove,
			MatChipRow,
			MatChipSet,
			MatChipTrailingIcon
		],
		exports: [
			BidiModule,
			MatChip,
			MatChipAvatar,
			MatChipEdit,
			MatChipEditInput,
			MatChipGrid,
			MatChipInput,
			MatChipListbox,
			MatChipOption,
			MatChipRemove,
			MatChipRow,
			MatChipSet,
			MatChipTrailingIcon
		]
	});
	static ɵinj = /*@__PURE__*/ ɵɵdefineInjector({
		providers: [ErrorStateMatcher, {
			provide: MAT_CHIPS_DEFAULT_OPTIONS,
			useValue: { separatorKeyCodes: [13] }
		}],
		imports: [MatRippleModule, BidiModule]
	});
};
(() => {
	(typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(MatChipsModule, [{
		type: NgModule,
		args: [{
			imports: [
				MatRippleModule,
				MatChipAction,
				CHIP_DECLARATIONS
			],
			exports: [BidiModule, CHIP_DECLARATIONS],
			providers: [ErrorStateMatcher, {
				provide: MAT_CHIPS_DEFAULT_OPTIONS,
				useValue: { separatorKeyCodes: [13] }
			}]
		}]
	}], null, null);
})();
export { MAT_CHIP, MAT_CHIPS_DEFAULT_OPTIONS, MAT_CHIP_AVATAR, MAT_CHIP_EDIT, MAT_CHIP_LISTBOX_CONTROL_VALUE_ACCESSOR, MAT_CHIP_REMOVE, MAT_CHIP_TRAILING_ICON, MatChip, MatChipAvatar, MatChipEdit, MatChipEditInput, MatChipGrid, MatChipGridChange, MatChipInput, MatChipListbox, MatChipListboxChange, MatChipOption, MatChipRemove, MatChipRow, MatChipSelectionChange, MatChipSet, MatChipTrailingIcon, MatChipsModule };
