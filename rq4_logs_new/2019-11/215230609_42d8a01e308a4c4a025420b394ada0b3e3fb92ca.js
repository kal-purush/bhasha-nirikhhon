    return fetchGet('/search/hot/detail')
export {};
import { VantComponent } from '../common/component';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea()],
    props: {
        show: Boolean,
        title: String,
        cancelText: String,
        customStyle: String,
        overlayStyle: String,
        zIndex: {
            type: Number,
            value: 100
        },
        actions: {
            type: Array,
            value: []
        },
        overlay: {
            type: Boolean,
            value: true
        },
        closeOnClickOverlay: {
            type: Boolean,
            value: true
        }
    },
    methods: {
        onSelect(event) {
            const { index } = event.currentTarget.dataset;
            const item = this.data.actions[index];
            if (item && !item.disabled && !item.loading) {
                this.$emit('select', item);
            }
        },
        onCancel() {
            this.$emit('cancel');
        },
        onClose() {
            this.$emit('close');
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { pickerProps } from '../picker/shared';
const COLUMNSPLACEHOLDERCODE = '000000';
VantComponent({
    classes: ['active-class', 'toolbar-class', 'column-class'],
    props: Object.assign({}, pickerProps, { value: String, areaList: {
            type: Object,
            value: {}
        }, columnsNum: {
            type: [String, Number],
            value: 3
        }, columnsPlaceholder: {
            type: Array,
            observer(val) {
                this.setData({
                    typeToColumnsPlaceholder: {
                        province: val[0] || '',
                        city: val[1] || '',
                        county: val[2] || '',
                    }
                });
            }
        } }),
    data: {
        columns: [{ values: [] }, { values: [] }, { values: [] }],
        displayColumns: [{ values: [] }, { values: [] }, { values: [] }],
        typeToColumnsPlaceholder: {}
    },
    watch: {
        value(value) {
            this.code = value;
            this.setValues();
        },
        areaList: 'setValues',
        columnsNum(value) {
            this.set({
                displayColumns: this.data.columns.slice(0, +value)
            });
        }
    },
    mounted() {
        setTimeout(() => {
            this.setValues();
        }, 0);
    },
    methods: {
        getPicker() {
            if (this.picker == null) {
                this.picker = this.selectComponent('.van-area__picker');
            }
            return this.picker;
        },
        onCancel(event) {
            this.emit('cancel', event.detail);
        },
        onConfirm(event) {
            const { index } = event.detail;
            let { value } = event.detail;
            value = this.parseOutputValues(value);
            this.emit('confirm', { value, index });
        },
        emit(type, detail) {
            detail.values = detail.value;
            delete detail.value;
            this.$emit(type, detail);
        },
        // parse output columns data
        parseOutputValues(values) {
            const { columnsPlaceholder } = this.data;
            return values.map((value, index) => {
                // save undefined value
                if (!value)
                    return value;
                value = JSON.parse(JSON.stringify(value));
                if (!value.code || value.name === columnsPlaceholder[index]) {
                    value.code = '';
                    value.name = '';
                }
                return value;
            });
        },
        onChange(event) {
            const { index, picker, value } = event.detail;
            this.code = value[index].code;
            let getValues = picker.getValues();
            getValues = this.parseOutputValues(getValues);
            this.setValues().then(() => {
                this.$emit('change', {
                    picker,
                    values: getValues,
                    index
                });
            });
        },
        getConfig(type) {
            const { areaList } = this.data;
            return (areaList && areaList[`${type}_list`]) || {};
        },
        getList(type, code) {
            const { typeToColumnsPlaceholder } = this.data;
            let result = [];
            if (type !== 'province' && !code) {
                return result;
            }
            const list = this.getConfig(type);
            result = Object.keys(list).map(code => ({
                code,
                name: list[code]
            }));
            if (code) {
                // oversea code
                if (code[0] === '9' && type === 'city') {
                    code = '9';
                }
                result = result.filter(item => item.code.indexOf(code) === 0);
            }
            if (typeToColumnsPlaceholder[type] && result.length) {
                // set columns placeholder
                const codeFill = type === 'province' ? '' : type === 'city' ? COLUMNSPLACEHOLDERCODE.slice(2, 4) : COLUMNSPLACEHOLDERCODE.slice(4, 6);
                result.unshift({
                    code: `${code}${codeFill}`,
                    name: typeToColumnsPlaceholder[type]
                });
            }
            return result;
        },
        getIndex(type, code) {
            let compareNum = type === 'province' ? 2 : type === 'city' ? 4 : 6;
            const list = this.getList(type, code.slice(0, compareNum - 2));
            // oversea code
            if (code[0] === '9' && type === 'province') {
                compareNum = 1;
            }
            code = code.slice(0, compareNum);
            for (let i = 0; i < list.length; i++) {
                if (list[i].code.slice(0, compareNum) === code) {
                    return i;
                }
            }
            return 0;
        },
        setValues() {
            const county = this.getConfig('county');
            let { code } = this;
            if (!code) {
                if (this.data.columnsPlaceholder.length) {
                    code = COLUMNSPLACEHOLDERCODE;
                }
                else if (Object.keys(county)[0]) {
                    code = Object.keys(county)[0];
                }
                else {
                    code = '';
                }
            }
            const province = this.getList('province');
            const city = this.getList('city', code.slice(0, 2));
            const picker = this.getPicker();
            if (!picker) {
                return;
            }
            const stack = [];
            stack.push(picker.setColumnValues(0, province, false));
            stack.push(picker.setColumnValues(1, city, false));
            if (city.length && code.slice(2, 4) === '00') {
                [{ code }] = city;
            }
            stack.push(picker.setColumnValues(2, this.getList('county', code.slice(0, 4)), false));
            return Promise.all(stack)
                .catch(() => { })
                .then(() => picker.setIndexes([
                this.getIndex('province', code),
                this.getIndex('city', code),
                this.getIndex('county', code)
            ]))
                .catch(() => { });
        },
        getValues() {
            const picker = this.getPicker();
            return picker ? picker.getValues().filter(value => !!value) : [];
        },
        getDetail() {
            const values = this.getValues();
            const area = {
                code: '',
                country: '',
                province: '',
                city: '',
                county: ''
            };
            if (!values.length) {
                return area;
            }
            const names = values.map((item) => item.name);
            area.code = values[values.length - 1].code;
            if (area.code[0] === '9') {
                area.country = names[1] || '';
                area.province = names[2] || '';
            }
            else {
                area.province = names[0] || '';
                area.city = names[1] || '';
                area.county = names[2] || '';
            }
            return area;
        },
        reset() {
            this.code = '';
            return this.setValues();
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        name: 'badge',
        type: 'descendant',
        linked(target) {
            this.badges.push(target);
            this.setActive(this.data.active);
        },
        unlinked(target) {
            this.badges = this.badges.filter(item => item !== target);
            this.setActive(this.data.active);
        }
    },
    props: {
        active: {
            type: Number,
            value: 0,
            observer: 'setActive'
        }
    },
    beforeCreate() {
        this.badges = [];
        this.currentActive = -1;
    },
    methods: {
        setActive(active) {
            const { badges, currentActive } = this;
            if (!badges.length) {
                return Promise.resolve();
            }
            this.currentActive = active;
            const stack = [];
            if (currentActive !== active && badges[currentActive]) {
                stack.push(badges[currentActive].setActive(false));
            }
            if (badges[active]) {
                stack.push(badges[active].setActive(true));
            }
            return Promise.all(stack);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        type: 'ancestor',
        name: 'badge-group',
        linked(target) {
            this.parent = target;
        }
    },
    props: {
        info: null,
        title: String
    },
    methods: {
        onClick() {
            const { parent } = this;
            if (!parent) {
                return;
            }
            const index = parent.badges.indexOf(this);
            parent.setActive(index).then(() => {
                this.$emit('click', index);
                parent.$emit('change', index);
            });
        },
        setActive(active) {
            return this.set({ active });
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { button } from '../mixins/button';
import { openType } from '../mixins/open-type';
VantComponent({
    mixins: [button, openType],
    classes: ['hover-class', 'loading-class'],
    props: {
        icon: String,
        color: String,
        plain: Boolean,
        block: Boolean,
        round: Boolean,
        square: Boolean,
        loading: Boolean,
        hairline: Boolean,
        disabled: Boolean,
        loadingText: String,
        type: {
            type: String,
            value: 'default'
        },
        size: {
            type: String,
            value: 'normal'
        },
        loadingSize: {
            type: String,
            value: '20px'
        }
    },
    methods: {
        onClick() {
            if (!this.data.disabled && !this.data.loading) {
                this.$emit('click');
            }
        }
    }
});
export {};
import { link } from '../mixins/link';
import { VantComponent } from '../common/component';
VantComponent({
    classes: [
        'num-class',
        'desc-class',
        'thumb-class',
        'title-class',
        'price-class',
        'origin-price-class',
    ],
    mixins: [link],
    props: {
        tag: String,
        num: String,
        desc: String,
        thumb: String,
        title: String,
        price: String,
        centered: Boolean,
        lazyLoad: Boolean,
        thumbLink: String,
        originPrice: String,
        thumbMode: {
            type: String,
            value: 'aspectFit'
        },
        currency: {
            type: String,
            value: '¥'
        }
    },
    methods: {
        onClickThumb() {
            this.jumpLink('thumbLink');
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        title: String,
        border: {
            type: Boolean,
            value: true
        }
    }
});
export {};
import { link } from '../mixins/link';
import { VantComponent } from '../common/component';
VantComponent({
    classes: [
        'title-class',
        'label-class',
        'value-class',
        'right-icon-class',
        'hover-class'
    ],
    mixins: [link],
    props: {
        title: null,
        value: null,
        icon: String,
        size: String,
        label: String,
        center: Boolean,
        isLink: Boolean,
        required: Boolean,
        clickable: Boolean,
        titleWidth: String,
        customStyle: String,
        arrowDirection: String,
        useLabelSlot: Boolean,
        border: {
            type: Boolean,
            value: true
        }
    },
    methods: {
        onClick(event) {
            this.$emit('click', event.detail);
            this.jumpLink();
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    relation: {
        name: 'checkbox',
        type: 'descendant',
        linked(target) {
            this.children = this.children || [];
            this.children.push(target);
            this.updateChild(target);
        },
        unlinked(target) {
            this.children = this.children.filter((child) => child !== target);
        }
    },
    props: {
        max: Number,
        value: {
            type: Array,
            observer: 'updateChildren'
        },
        disabled: {
            type: Boolean,
            observer: 'updateChildren'
        }
    },
    methods: {
        updateChildren() {
            (this.children || []).forEach((child) => this.updateChild(child));
        },
        updateChild(child) {
            const { value, disabled } = this.data;
            child.set({
                value: value.indexOf(child.data.name) !== -1,
                disabled: disabled || child.data.disabled
            });
        }
    }
});
export {};
import { VantComponent } from '../common/component';
function emit(target, value) {
    target.$emit('input', value);
    target.$emit('change', value);
}
VantComponent({
    field: true,
    relation: {
        name: 'checkbox-group',
        type: 'ancestor',
        linked(target) {
            this.parent = target;
        },
        unlinked() {
            this.parent = null;
        }
    },
    classes: ['icon-class', 'label-class'],
    props: {
        value: Boolean,
        disabled: Boolean,
        useIconSlot: Boolean,
        checkedColor: String,
        labelPosition: String,
        labelDisabled: Boolean,
        shape: {
            type: String,
            value: 'round'
        }
    },
    methods: {
        emitChange(value) {
            if (this.parent) {
                this.setParentValue(this.parent, value);
            }
            else {
                emit(this, value);
            }
        },
        toggle() {
            const { disabled, value } = this.data;
            if (!disabled) {
                this.emitChange(!value);
            }
        },
        onClickLabel() {
            const { labelDisabled, disabled, value } = this.data;
            if (!disabled && !labelDisabled) {
                this.emitChange(!value);
            }
        },
        setParentValue(parent, value) {
            const parentValue = parent.data.value.slice();
            const { name } = this.data;
            const { max } = parent.data;
            if (value) {
                if (max && parentValue.length >= max) {
                    return;
                }
                if (parentValue.indexOf(name) === -1) {
                    parentValue.push(name);
                    emit(parent, parentValue);
                }
            }
            else {
                const index = parentValue.indexOf(name);
                if (index !== -1) {
                    parentValue.splice(index, 1);
                    emit(parent, parentValue);
                }
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        name: 'row',
        type: 'ancestor'
    },
    props: {
        span: Number,
        offset: Number
    },
    data: {
        style: ''
    },
    methods: {
        setGutter(gutter) {
            const padding = `${gutter / 2}px`;
            const style = gutter ? `padding-left: ${padding}; padding-right: ${padding};` : '';
            if (style !== this.data.style) {
                this.set({ style });
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
const nextTick = () => new Promise(resolve => setTimeout(resolve, 20));
VantComponent({
    classes: ['title-class', 'content-class'],
    relation: {
        name: 'collapse',
        type: 'ancestor',
        linked(parent) {
            this.parent = parent;
        }
    },
    props: {
        name: null,
        title: null,
        value: null,
        icon: String,
        label: String,
        disabled: Boolean,
        clickable: Boolean,
        border: {
            type: Boolean,
            value: true
        },
        isLink: {
            type: Boolean,
            value: true
        }
    },
    data: {
        contentHeight: 0,
        expanded: false,
        transition: false
    },
    mounted() {
        this.updateExpanded()
            .then(nextTick)
            .then(() => {
            const data = { transition: true };
            if (this.data.expanded) {
                data.contentHeight = 'auto';
            }
            this.set(data);
        });
    },
    methods: {
        updateExpanded() {
            if (!this.parent) {
                return Promise.resolve();
            }
            const { value, accordion } = this.parent.data;
            const { children = [] } = this.parent;
            const { name } = this.data;
            const index = children.indexOf(this);
            const currentName = name == null ? index : name;
            const expanded = accordion
                ? value === currentName
                : (value || []).some((name) => name === currentName);
            const stack = [];
            if (expanded !== this.data.expanded) {
                stack.push(this.updateStyle(expanded));
            }
            stack.push(this.set({ index, expanded }));
            return Promise.all(stack);
        },
        updateStyle(expanded) {
            return this.getRect('.van-collapse-item__content')
                .then((rect) => rect.height)
                .then((height) => {
                if (expanded) {
                    return this.set({
                        contentHeight: height ? `${height}px` : 'auto'
                    });
                }
                return this.set({ contentHeight: `${height}px` })
                    .then(nextTick)
                    .then(() => this.set({ contentHeight: 0 }));
            });
        },
        onClick() {
            if (this.data.disabled) {
                return;
            }
            const { name, expanded } = this.data;
            const index = this.parent.children.indexOf(this);
            const currentName = name == null ? index : name;
            this.parent.switch(currentName, !expanded);
        },
        onTransitionEnd() {
            if (this.data.expanded) {
                this.set({
                    contentHeight: 'auto'
                });
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        name: 'collapse-item',
        type: 'descendant',
        linked(child) {
            this.children.push(child);
        },
        unlinked(child) {
            this.children = this.children.filter((item) => item !== child);
        }
    },
    props: {
        value: {
            type: null,
            observer: 'updateExpanded'
        },
        accordion: {
            type: Boolean,
            observer: 'updateExpanded'
        },
        border: {
            type: Boolean,
            value: true
        }
    },
    beforeCreate() {
        this.children = [];
    },
    methods: {
        updateExpanded() {
            this.children.forEach((child) => {
                child.updateExpanded();
            });
        },
        switch(name, expanded) {
            const { accordion, value } = this.data;
            if (!accordion) {
                name = expanded
                    ? (value || []).concat(name)
                    : (value || []).filter((activeName) => activeName !== name);
            }
            else {
                name = expanded ? name : '';
            }
            this.$emit('change', name);
            this.$emit('input', name);
        }
    }
});
export declare const RED = "#f44";
export declare const BLUE = "#1989fa";
export declare const GREEN = "#07c160";
export declare const ORANGE = "#ff976a";
export const RED = '#f44';
export const BLUE = '#1989fa';
export const GREEN = '#07c160';
export const ORANGE = '#ff976a';
import { VantComponentOptions, CombinedComponentInstance } from '../definitions/index';
declare function VantComponent<Data, Props, Methods, Computed>(vantOptions?: VantComponentOptions<Data, Props, Methods, Computed, CombinedComponentInstance<Data, Props, Methods, Computed>>): void;
export { VantComponent };
import { basic } from '../mixins/basic';
import { observe } from '../mixins/observer/index';
function mapKeys(source, target, map) {
    Object.keys(map).forEach(key => {
        if (source[key]) {
            target[map[key]] = source[key];
        }
    });
}
function VantComponent(vantOptions = {}) {
    const options = {};
    mapKeys(vantOptions, options, {
        data: 'data',
        props: 'properties',
        mixins: 'behaviors',
        methods: 'methods',
        beforeCreate: 'created',
        created: 'attached',
        mounted: 'ready',
        relations: 'relations',
        destroyed: 'detached',
        classes: 'externalClasses'
    });
    const { relation } = vantOptions;
    if (relation) {
        options.relations = Object.assign(options.relations || {}, {
            [`../${relation.name}/index`]: relation
        });
    }
    // add default externalClasses
    options.externalClasses = options.externalClasses || [];
    options.externalClasses.push('custom-class');
    // add default behaviors
    options.behaviors = options.behaviors || [];
    options.behaviors.push(basic);
    // map field to form-field behavior
    if (vantOptions.field) {
        options.behaviors.push('wx://form-field');
    }
    // add default options
    options.options = {
        multipleSlots: true,
        addGlobalClass: true
    };
    observe(vantOptions, options);
    Component(options);
}
export { VantComponent };
/// <reference types="miniprogram-api-typings" />
export declare function isDef(value: any): boolean;
export declare function isObj(x: any): boolean;
export declare function isNumber(value: any): boolean;
export declare function range(num: number, min: number, max: number): number;
export declare function nextTick(fn: Function): void;
export declare function getSystemInfoSync(): WechatMiniprogram.GetSystemInfoSuccessCallbackResult;
export function isDef(value) {
    return value !== undefined && value !== null;
}
export function isObj(x) {
    const type = typeof x;
    return x !== null && (type === 'object' || type === 'function');
}
export function isNumber(value) {
    return /^\d+$/.test(value);
}
export function range(num, min, max) {
    return Math.min(Math.max(num, min), max);
}
export function nextTick(fn) {
    setTimeout(() => {
        fn();
    }, 1000 / 30);
}
let systemInfo = null;
export function getSystemInfoSync() {
    if (systemInfo == null) {
        systemInfo = wx.getSystemInfoSync();
    }
    return systemInfo;
}
export {};
import { VantComponent } from '../common/component';
import { isDef } from '../common/utils';
import { pickerProps } from '../picker/shared';
const currentYear = new Date().getFullYear();
function isValidDate(date) {
    return isDef(date) && !isNaN(new Date(date).getTime());
}
function range(num, min, max) {
    return Math.min(Math.max(num, min), max);
}
function padZero(val) {
    return `00${val}`.slice(-2);
}
function times(n, iteratee) {
    let index = -1;
    const result = Array(n < 0 ? 0 : n);
    while (++index < n) {
        result[index] = iteratee(index);
    }
    return result;
}
function getTrueValue(formattedValue) {
    if (!formattedValue)
        return;
    while (isNaN(parseInt(formattedValue, 10))) {
        formattedValue = formattedValue.slice(1);
    }
    return parseInt(formattedValue, 10);
}
function getMonthEndDay(year, month) {
    return 32 - new Date(year, month - 1, 32).getDate();
}
const defaultFormatter = (_, value) => value;
VantComponent({
    classes: ['active-class', 'toolbar-class', 'column-class'],
    props: Object.assign({}, pickerProps, { formatter: {
            type: Function,
            value: defaultFormatter
        }, value: null, type: {
            type: String,
            value: 'datetime'
        }, showToolbar: {
            type: Boolean,
            value: true
        }, minDate: {
            type: Number,
            value: new Date(currentYear - 10, 0, 1).getTime()
        }, maxDate: {
            type: Number,
            value: new Date(currentYear + 10, 11, 31).getTime()
        }, minHour: {
            type: Number,
            value: 0
        }, maxHour: {
            type: Number,
            value: 23
        }, minMinute: {
            type: Number,
            value: 0
        }, maxMinute: {
            type: Number,
            value: 59
        } }),
    data: {
        innerValue: Date.now(),
        columns: []
    },
    watch: {
        value: 'updateValue',
        type: 'updateValue',
        minDate: 'updateValue',
        maxDate: 'updateValue',
        minHour: 'updateValue',
        maxHour: 'updateValue',
        minMinute: 'updateValue',
        maxMinute: 'updateValue'
    },
    methods: {
        updateValue() {
            const { data } = this;
            const val = this.correctValue(this.data.value);
            const isEqual = val === data.innerValue;
            if (!isEqual) {
                this.updateColumnValue(val).then(() => {
                    this.$emit('input', val);
                });
            }
            else {
                this.updateColumns();
            }
        },
        getPicker() {
            if (this.picker == null) {
                this.picker = this.selectComponent('.van-datetime-picker');
                const { picker } = this;
                const { setColumnValues } = picker;
                picker.setColumnValues = (...args) => setColumnValues.apply(picker, [...args, false]);
            }
            return this.picker;
        },
        updateColumns() {
            const { formatter = defaultFormatter } = this.data;
            const results = this.getRanges().map(({ type, range }) => {
                const values = times(range[1] - range[0] + 1, index => {
                    let value = range[0] + index;
                    value = type === 'year' ? `${value}` : padZero(value);
                    return formatter(type, value);
                });
                return { values };
            });
            return this.set({ columns: results });
        },
        getRanges() {
            const { data } = this;
            if (data.type === 'time') {
                return [
                    {
                        type: 'hour',
                        range: [data.minHour, data.maxHour]
                    },
                    {
                        type: 'minute',
                        range: [data.minMinute, data.maxMinute]
                    }
                ];
            }
            const { maxYear, maxDate, maxMonth, maxHour, maxMinute } = this.getBoundary('max', data.innerValue);
            const { minYear, minDate, minMonth, minHour, minMinute } = this.getBoundary('min', data.innerValue);
            const result = [
                {
                    type: 'year',
                    range: [minYear, maxYear]
                },
                {
                    type: 'month',
                    range: [minMonth, maxMonth]
                },
                {
                    type: 'day',
                    range: [minDate, maxDate]
                },
                {
                    type: 'hour',
                    range: [minHour, maxHour]
                },
                {
                    type: 'minute',
                    range: [minMinute, maxMinute]
                }
            ];
            if (data.type === 'date')
                result.splice(3, 2);
            if (data.type === 'year-month')
                result.splice(2, 3);
            return result;
        },
        correctValue(value) {
            const { data } = this;
            // validate value
            const isDateType = data.type !== 'time';
            if (isDateType && !isValidDate(value)) {
                value = data.minDate;
            }
            else if (!isDateType && !value) {
                const { minHour } = data;
                value = `${padZero(minHour)}:00`;
            }
            // time type
            if (!isDateType) {
                let [hour, minute] = value.split(':');
                hour = padZero(range(hour, data.minHour, data.maxHour));
                minute = padZero(range(minute, data.minMinute, data.maxMinute));
                return `${hour}:${minute}`;
            }
            // date type
            value = Math.max(value, data.minDate);
            value = Math.min(value, data.maxDate);
            return value;
        },
        getBoundary(type, innerValue) {
            const value = new Date(innerValue);
            const boundary = new Date(this.data[`${type}Date`]);
            const year = boundary.getFullYear();
            let month = 1;
            let date = 1;
            let hour = 0;
            let minute = 0;
            if (type === 'max') {
                month = 12;
                date = getMonthEndDay(value.getFullYear(), value.getMonth() + 1);
                hour = 23;
                minute = 59;
            }
            if (value.getFullYear() === year) {
                month = boundary.getMonth() + 1;
                if (value.getMonth() + 1 === month) {
                    date = boundary.getDate();
                    if (value.getDate() === date) {
                        hour = boundary.getHours();
                        if (value.getHours() === hour) {
                            minute = boundary.getMinutes();
                        }
                    }
                }
            }
            return {
                [`${type}Year`]: year,
                [`${type}Month`]: month,
                [`${type}Date`]: date,
                [`${type}Hour`]: hour,
                [`${type}Minute`]: minute
            };
        },
        onCancel() {
            this.$emit('cancel');
        },
        onConfirm() {
            this.$emit('confirm', this.data.innerValue);
        },
        onChange() {
            const { data } = this;
            let value;
            const picker = this.getPicker();
            if (data.type === 'time') {
                const indexes = picker.getIndexes();
                value = `${indexes[0] + data.minHour}:${indexes[1] + data.minMinute}`;
            }
            else {
                const values = picker.getValues();
                const year = getTrueValue(values[0]);
                const month = getTrueValue(values[1]);
                const maxDate = getMonthEndDay(year, month);
                let date = getTrueValue(values[2]);
                if (data.type === 'year-month') {
                    date = 1;
                }
                date = date > maxDate ? maxDate : date;
                let hour = 0;
                let minute = 0;
                if (data.type === 'datetime') {
                    hour = getTrueValue(values[3]);
                    minute = getTrueValue(values[4]);
                }
                value = new Date(year, month - 1, date, hour, minute);
            }
            value = this.correctValue(value);
            this.updateColumnValue(value).then(() => {
                this.$emit('input', value);
                this.$emit('change', picker);
            });
        },
        updateColumnValue(value) {
            let values = [];
            const { type, formatter = defaultFormatter } = this.data;
            const picker = this.getPicker();
            if (type === 'time') {
                const pair = value.split(':');
                values = [
                    formatter('hour', pair[0]),
                    formatter('minute', pair[1])
                ];
            }
            else {
                const date = new Date(value);
                values = [
                    formatter('year', `${date.getFullYear()}`),
                    formatter('month', padZero(date.getMonth() + 1))
                ];
                if (type === 'date') {
                    values.push(formatter('day', padZero(date.getDate())));
                }
                if (type === 'datetime') {
                    values.push(formatter('day', padZero(date.getDate())), formatter('hour', padZero(date.getHours())), formatter('minute', padZero(date.getMinutes())));
                }
            }
            return this.set({ innerValue: value })
                .then(() => this.updateColumns())
                .then(() => picker.setValues(values));
        }
    },
    created() {
        const innerValue = this.correctValue(this.data.value);
        this.updateColumnValue(innerValue).then(() => {
            this.$emit('input', innerValue);
        });
    }
});
/// <reference types="miniprogram-api-typings" />
import { Weapp } from './weapp';
declare type RecordToAny<T> = {
    [K in keyof T]: any;
};
declare type RecordToReturn<T> = {
    [P in keyof T]: T[P] extends (...args: any[]) => any ? ReturnType<T[P]> : T[P];
};
export declare type CombinedComponentInstance<Data, Props, Methods, Computed> = Methods & WechatMiniprogram.Component.TrivialInstance & Weapp.FormField & {
    data: Data & RecordToReturn<Computed> & RecordToAny<Props>;
};
export interface VantComponentOptions<Data, Props, Methods, Computed, Instance> {
    data?: Data;
    field?: boolean;
    classes?: string[];
    mixins?: string[];
    props?: Props & Weapp.PropertyOption;
    watch?: Weapp.WatchOption<Instance>;
    computed?: Computed & Weapp.ComputedOption<Instance>;
    relation?: Weapp.RelationOption<Instance> & {
        name: string;
    };
    relations?: {
        [componentName: string]: Weapp.RelationOption<Instance>;
    };
    methods?: Methods & Weapp.MethodOption<Instance>;
    beforeCreate?: (this: Instance) => void;
    created?: (this: Instance) => void;
    mounted?: (this: Instance) => void;
    destroyed?: (this: Instance) => void;
}
export {};
/// <reference types="miniprogram-api-typings" />
export declare namespace Weapp {
    interface FormField {
        data: {
            name: string;
            value: any;
        };
    }
    interface Target {
        id: string;
        tagName: string;
        dataset: {
            [key: string]: any;
        };
    }
    interface Event {
        /**
         * 代表事件的类型。
         */
        type: string;
        /**
         * 页面打开到触发事件所经过的毫秒数。
         */
        timeStamp: number;
        /**
         * 触发事件的源组件。
         */
        target: Target;
        /**
         * 事件绑定的当前组件。
         */
        currentTarget: Target;
        /**
         * 额外的信息
         */
        detail: any;
    }
    interface Touch {
        /**
         * 触摸点的标识符
         */
        identifier: number;
        /**
         * 距离文档左上角的距离，文档的左上角为原点 ，横向为X轴，纵向为Y轴
         */
        pageX: number;
        /**
         * 距离文档左上角的距离，文档的左上角为原点 ，横向为X轴，纵向为Y轴
         */
        pageY: number;
        /**
         * 距离页面可显示区域（屏幕除去导航条）左上角距离，横向为X轴，纵向为Y轴
         */
        clientX: number;
        /**
         * 距离页面可显示区域（屏幕除去导航条）左上角距离，横向为X轴，纵向为Y轴
         */
        clientY: number;
    }
    interface TouchEvent extends Event {
        touches: Array<Touch>;
        changedTouches: Array<Touch>;
    }
    /**
     * relation定义，miniprogram-api-typings缺少this定义
     */
    interface RelationOption<Instance> {
        /** 目标组件的相对关系 */
        type: 'parent' | 'child' | 'ancestor' | 'descendant';
        /** 关系生命周期函数，当关系被建立在页面节点树中时触发，触发时机在组件attached生命周期之后 */
        linked?(this: Instance, target: WechatMiniprogram.Component.TrivialInstance): void;
        /** 关系生命周期函数，当关系在页面节点树中发生改变时触发，触发时机在组件moved生命周期之后 */
        linkChanged?(this: Instance, target: WechatMiniprogram.Component.TrivialInstance): void;
        /** 关系生命周期函数，当关系脱离页面节点树时触发，触发时机在组件detached生命周期之后 */
        unlinked?(this: Instance, target: WechatMiniprogram.Component.TrivialInstance): void;
        /** 如果这一项被设置，则它表示关联的目标节点所应具有的behavior，所有拥有这一behavior的组件节点都会被关联 */
        target?: string;
    }
    /**
     * obverser定义，miniprogram-api-typings缺少this定义
     */
    type Observer<Instance, T> = (this: Instance, newVal: T, oldVal: T, changedPath: Array<string | number>) => void;
    /**
     * watch定义
     */
    interface WatchOption<Instance> {
        [name: string]: string | Observer<Instance, any>;
    }
    /**
     * methods定义，miniprogram-api-typings缺少this定义
     */
    interface MethodOption<Instance> {
        [name: string]: (this: Instance, ...args: any[]) => any;
    }
    interface ComputedOption<Instance> {
        [name: string]: (this: Instance) => any;
    }
    type PropertyType = StringConstructor | NumberConstructor | BooleanConstructor | ArrayConstructor | ObjectConstructor | FunctionConstructor | null;
    interface PropertyOption {
        [name: string]: PropertyType | PropertyType[] | {
            /** 属性类型 */
            type: PropertyType | PropertyType[];
            /** 属性初始值 */
            value?: any;
            /** 属性值被更改时的响应函数 */
            observer?: string | Observer<WechatMiniprogram.Component.TrivialInstance, any>;
            /** 属性的类型（可以指定多个） */
            optionalTypes?: PropertyType[];
        };
    }
}
/// <reference types="miniprogram-api-typings" />
declare type DialogAction = 'confirm' | 'cancel';
declare type DialogOptions = {
    lang?: string;
    show?: boolean;
    title?: string;
    zIndex?: number;
    context?: WechatMiniprogram.Page.TrivialInstance | WechatMiniprogram.Component.TrivialInstance;
    message?: string;
    overlay?: boolean;
    selector?: string;
    ariaLabel?: string;
    className?: string;
    customStyle?: string;
    transition?: string;
    asyncClose?: boolean;
    businessId?: number;
    sessionFrom?: string;
    appParameter?: string;
    messageAlign?: string;
    sendMessageImg?: string;
    showMessageCard?: boolean;
    sendMessagePath?: string;
    sendMessageTitle?: string;
    confirmButtonText?: string;
    cancelButtonText?: string;
    showConfirmButton?: boolean;
    showCancelButton?: boolean;
    closeOnClickOverlay?: boolean;
    confirmButtonOpenType?: string;
};
interface Dialog {
    (options: DialogOptions): Promise<DialogAction>;
    alert?: (options: DialogOptions) => Promise<DialogAction>;
    confirm?: (options: DialogOptions) => Promise<DialogAction>;
    close?: () => void;
    stopLoading?: () => void;
    install?: () => void;
    setDefaultOptions?: (options: DialogOptions) => void;
    resetDefaultOptions?: () => void;
    defaultOptions?: DialogOptions;
    currentOptions?: DialogOptions;
}
declare const Dialog: Dialog;
export default Dialog;
let queue = [];
function getContext() {
    const pages = getCurrentPages();
    return pages[pages.length - 1];
}
const Dialog = options => {
    options = Object.assign({}, Dialog.currentOptions, options);
    return new Promise((resolve, reject) => {
        const context = options.context || getContext();
        const dialog = context.selectComponent(options.selector);
        delete options.context;
        delete options.selector;
        if (dialog) {
            dialog.set(Object.assign({ onCancel: reject, onConfirm: resolve }, options));
            queue.push(dialog);
        }
        else {
            console.warn('未找到 van-dialog 节点，请确认 selector 及 context 是否正确');
        }
    });
};
Dialog.defaultOptions = {
    show: true,
    title: '',
    message: '',
    zIndex: 100,
    overlay: true,
    className: '',
    customStyle: '',
    asyncClose: false,
    messageAlign: '',
    transition: 'scale',
    selector: '#van-dialog',
    confirmButtonText: '确认',
    cancelButtonText: '取消',
    showConfirmButton: true,
    showCancelButton: false,
    closeOnClickOverlay: false,
    confirmButtonOpenType: ''
};
Dialog.alert = Dialog;
Dialog.confirm = options => Dialog(Object.assign({ showCancelButton: true }, options));
Dialog.close = () => {
    queue.forEach(dialog => {
        dialog.close();
    });
    queue = [];
};
Dialog.stopLoading = () => {
    queue.forEach(dialog => {
        dialog.stopLoading();
    });
};
Dialog.setDefaultOptions = options => {
    Object.assign(Dialog.currentOptions, options);
};
Dialog.resetDefaultOptions = () => {
    Dialog.currentOptions = Object.assign({}, Dialog.defaultOptions);
};
Dialog.resetDefaultOptions();
export default Dialog;
export {};
import { VantComponent } from '../common/component';
import { button } from '../mixins/button';
import { openType } from '../mixins/open-type';
VantComponent({
    mixins: [button, openType],
    props: {
        show: Boolean,
        title: String,
        message: String,
        useSlot: Boolean,
        className: String,
        customStyle: String,
        asyncClose: Boolean,
        messageAlign: String,
        showCancelButton: Boolean,
        closeOnClickOverlay: Boolean,
        confirmButtonOpenType: String,
        zIndex: {
            type: Number,
            value: 2000
        },
        confirmButtonText: {
            type: String,
            value: '确认'
        },
        cancelButtonText: {
            type: String,
            value: '取消'
        },
        showConfirmButton: {
            type: Boolean,
            value: true
        },
        overlay: {
            type: Boolean,
            value: true
        },
        transition: {
            type: String,
            value: 'scale'
        }
    },
    data: {
        loading: {
            confirm: false,
            cancel: false
        }
    },
    watch: {
        show(show) {
            !show && this.stopLoading();
        }
    },
    methods: {
        onConfirm() {
            this.handleAction('confirm');
        },
        onCancel() {
            this.handleAction('cancel');
        },
        onClickOverlay() {
            this.onClose('overlay');
        },
        handleAction(action) {
            if (this.data.asyncClose) {
                this.set({
                    [`loading.${action}`]: true
                });
            }
            this.onClose(action);
        },
        close() {
            this.set({
                show: false
            });
        },
        stopLoading() {
            this.set({
                loading: {
                    confirm: false,
                    cancel: false
                }
            });
        },
        onClose(action) {
            if (!this.data.asyncClose) {
                this.close();
            }
            this.$emit('close', action);
            // 把 dialog 实例传递出去，可以通过 stopLoading() 在外部关闭按钮的 loading
            this.$emit(action, { dialog: this });
            const callback = this.data[action === 'confirm' ? 'onConfirm' : 'onCancel'];
            if (callback) {
                callback(this);
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { getSystemInfoSync } from '../common/utils';
VantComponent({
    field: true,
    classes: ['input-class', 'right-icon-class'],
    props: {
        size: String,
        icon: String,
        label: String,
        error: Boolean,
        fixed: Boolean,
        focus: Boolean,
        center: Boolean,
        isLink: Boolean,
        leftIcon: String,
        rightIcon: String,
        disabled: Boolean,
        autosize: Boolean,
        readonly: Boolean,
        required: Boolean,
        password: Boolean,
        iconClass: String,
        clearable: Boolean,
        inputAlign: String,
        customStyle: String,
        confirmType: String,
        confirmHold: Boolean,
        errorMessage: String,
        placeholder: String,
        placeholderStyle: String,
        errorMessageAlign: String,
        selectionEnd: {
            type: Number,
            value: -1
        },
        selectionStart: {
            type: Number,
            value: -1
        },
        showConfirmBar: {
            type: Boolean,
            value: true
        },
        adjustPosition: {
            type: Boolean,
            value: true
        },
        cursorSpacing: {
            type: Number,
            value: 50
        },
        maxlength: {
            type: Number,
            value: -1
        },
        type: {
            type: String,
            value: 'text'
        },
        border: {
            type: Boolean,
            value: true
        },
        titleWidth: {
            type: String,
            value: '90px'
        }
    },
    data: {
        focused: false,
        system: getSystemInfoSync().system.split(' ').shift().toLowerCase()
    },
    methods: {
        onInput(event) {
            const { value = '' } = event.detail || {};
            this.set({ value }, () => {
                this.emitChange(value);
            });
        },
        onFocus(event) {
            this.set({ focused: true });
            this.$emit('focus', event.detail);
        },
        onBlur(event) {
            this.set({ focused: false });
            this.$emit('blur', event.detail);
        },
        onClickIcon() {
            this.$emit('click-icon');
        },
        onClear() {
            this.set({ value: '' }, () => {
                this.emitChange('');
                this.$emit('clear', '');
            });
        },
        onConfirm() {
            this.$emit('confirm', this.data.value);
        },
        emitChange(value) {
            this.$emit('input', value);
            this.$emit('change', value);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { link } from '../mixins/link';
import { button } from '../mixins/button';
import { openType } from '../mixins/open-type';
VantComponent({
    mixins: [link, button, openType],
    props: {
        text: String,
        loading: Boolean,
        disabled: Boolean,
        type: {
            type: String,
            value: 'danger'
        }
    },
    methods: {
        onClick(event) {
            this.$emit('click', event.detail);
            this.jumpLink();
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { link } from '../mixins/link';
import { button } from '../mixins/button';
import { openType } from '../mixins/open-type';
VantComponent({
    classes: ['icon-class', 'text-class'],
    mixins: [link, button, openType],
    props: {
        text: String,
        info: String,
        icon: String,
        disabled: Boolean,
        loading: Boolean
    },
    methods: {
        onClick(event) {
            this.$emit('click', event.detail);
            this.jumpLink();
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea()]
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        info: null,
        name: String,
        size: String,
        color: String,
        customStyle: String,
        classPrefix: {
            type: String,
            value: 'van-icon'
        }
    },
    methods: {
        onClick() {
            this.$emit('click');
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        info: null,
        customStyle: String
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        size: {
            type: String,
            value: '30px'
        },
        type: {
            type: String,
            value: 'circular'
        },
        color: {
            type: String,
            value: '#c9c9c9'
        }
    }
});
export declare const basic: string;
export const basic = Behavior({
    methods: {
        $emit(...args) {
            this.triggerEvent(...args);
        },
        getRect(selector, all) {
            return new Promise(resolve => {
                wx.createSelectorQuery()
                    .in(this)[all ? 'selectAll' : 'select'](selector)
                    .boundingClientRect(rect => {
                    if (all && Array.isArray(rect) && rect.length) {
                        resolve(rect);
                    }
                    if (!all && rect) {
                        resolve(rect);
                    }
                })
                    .exec();
            });
        }
    }
});
export declare const button: string;
export const button = Behavior({
    externalClasses: ['hover-class'],
    properties: {
        id: String,
        lang: {
            type: String,
            value: 'en'
        },
        businessId: Number,
        sessionFrom: String,
        sendMessageTitle: String,
        sendMessagePath: String,
        sendMessageImg: String,
        showMessageCard: Boolean,
        appParameter: String,
        ariaLabel: String
    }
});
export declare const link: string;
export const link = Behavior({
    properties: {
        url: String,
        linkType: {
            type: String,
            value: 'navigateTo'
        }
    },
    methods: {
        jumpLink(urlKey = 'url') {
            const url = this.data[urlKey];
            if (url) {
                wx[this.data.linkType]({ url });
            }
        }
    }
});
export declare const behavior: string;
function setAsync(context, data) {
    return new Promise(resolve => {
        context.setData(data, resolve);
    });
}
export const behavior = Behavior({
    created() {
        if (!this.$options) {
            return;
        }
        const cache = {};
        const { computed } = this.$options();
        const keys = Object.keys(computed);
        this.calcComputed = () => {
            const needUpdate = {};
            keys.forEach(key => {
                const value = computed[key].call(this);
                if (cache[key] !== value) {
                    cache[key] = value;
                    needUpdate[key] = value;
                }
            });
            return needUpdate;
        };
    },
    attached() {
        this.set();
    },
    methods: {
        // set data and set computed data
        set(data, callback) {
            const stack = [];
            if (data) {
                stack.push(setAsync(this, data));
            }
            if (this.calcComputed) {
                stack.push(setAsync(this, this.calcComputed()));
            }
            return Promise.all(stack).then(res => {
                if (callback && typeof callback === 'function') {
                    callback.call(this);
                }
                return res;
            });
        }
    }
});
export declare function observe(vantOptions: any, options: any): void;
import { behavior } from './behavior';
import { observeProps } from './props';
export function observe(vantOptions, options) {
    const { watch, computed } = vantOptions;
    options.behaviors.push(behavior);
    if (watch) {
        const props = options.properties || {};
        Object.keys(watch).forEach(key => {
            if (key in props) {
                let prop = props[key];
                if (prop === null || !('type' in prop)) {
                    prop = { type: prop };
                }
                prop.observer = watch[key];
                props[key] = prop;
            }
        });
        options.properties = props;
    }
    if (computed) {
        options.methods = options.methods || {};
        options.methods.$options = () => vantOptions;
        if (options.properties) {
            observeProps(options.properties);
        }
    }
}
export declare function observeProps(props: any): void;
export function observeProps(props) {
    if (!props) {
        return;
    }
    Object.keys(props).forEach(key => {
        let prop = props[key];
        if (prop === null || !('type' in prop)) {
            prop = { type: prop };
        }
        let { observer } = prop;
        prop.observer = function (...args) {
            if (observer) {
                if (typeof observer === 'string') {
                    observer = this[observer];
                }
                observer.apply(this, args);
            }
            this.set();
        };
        props[key] = prop;
    });
}
export declare const openType: string;
export const openType = Behavior({
    properties: {
        openType: String
    },
    methods: {
        bindGetUserInfo(event) {
            this.$emit('getuserinfo', event.detail);
        },
        bindContact(event) {
            this.$emit('contact', event.detail);
        },
        bindGetPhoneNumber(event) {
            this.$emit('getphonenumber', event.detail);
        },
        bindError(event) {
            this.$emit('error', event.detail);
        },
        bindLaunchApp(event) {
            this.$emit('launchapp', event.detail);
        },
        bindOpenSetting(event) {
            this.$emit('opensetting', event.detail);
        },
    }
});
export declare const safeArea: ({ safeAreaInsetBottom, safeAreaInsetTop }?: {
    safeAreaInsetBottom?: boolean;
    safeAreaInsetTop?: boolean;
}) => string;
let cache = null;
function getSafeArea() {
    return new Promise((resolve, reject) => {
        if (cache != null) {
            resolve(cache);
        }
        else {
            wx.getSystemInfo({
                success: ({ model, screenHeight, statusBarHeight }) => {
                    const iphoneX = /iphone x/i.test(model);
                    const iphoneNew = /iPhone11/i.test(model) && screenHeight === 812;
                    cache = {
                        isIPhoneX: iphoneX || iphoneNew,
                        statusBarHeight
                    };
                    resolve(cache);
                },
                fail: reject
            });
        }
    });
}
export const safeArea = ({ safeAreaInsetBottom = true, safeAreaInsetTop = false } = {}) => Behavior({
    properties: {
        safeAreaInsetTop: {
            type: Boolean,
            value: safeAreaInsetTop
        },
        safeAreaInsetBottom: {
            type: Boolean,
            value: safeAreaInsetBottom
        }
    },
    created() {
        getSafeArea().then(({ isIPhoneX, statusBarHeight }) => {
            this.set({ isIPhoneX, statusBarHeight });
        });
    }
});
export declare const touch: string;
export const touch = Behavior({
    methods: {
        touchStart(event) {
            const touch = event.touches[0];
            this.direction = '';
            this.deltaX = 0;
            this.deltaY = 0;
            this.offsetX = 0;
            this.offsetY = 0;
            this.startX = touch.clientX;
            this.startY = touch.clientY;
        },
        touchMove(event) {
            const touch = event.touches[0];
            this.deltaX = touch.clientX - this.startX;
            this.deltaY = touch.clientY - this.startY;
            this.offsetX = Math.abs(this.deltaX);
            this.offsetY = Math.abs(this.deltaY);
            this.direction =
                this.offsetX > this.offsetY
                    ? 'horizontal'
                    : this.offsetX < this.offsetY
                        ? 'vertical'
                        : '';
        }
    }
});
export declare const transition: (showDefaultValue: boolean) => any;
import { isObj } from '../common/utils';
const getClassNames = (name) => ({
    enter: `van-${name}-enter van-${name}-enter-active enter-class enter-active-class`,
    'enter-to': `van-${name}-enter-to van-${name}-enter-active enter-to-class enter-active-class`,
    leave: `van-${name}-leave van-${name}-leave-active leave-class leave-active-class`,
    'leave-to': `van-${name}-leave-to van-${name}-leave-active leave-to-class leave-active-class`
});
const nextTick = () => new Promise(resolve => setTimeout(resolve, 1000 / 30));
export const transition = function (showDefaultValue) {
    return Behavior({
        properties: {
            customStyle: String,
            // @ts-ignore
            show: {
                type: Boolean,
                value: showDefaultValue,
                observer: 'observeShow'
            },
            // @ts-ignore
            duration: {
                type: [Number, Object],
                value: 300,
                observer: 'observeDuration'
            },
            name: {
                type: String,
                value: 'fade'
            }
        },
        data: {
            type: '',
            inited: false,
            display: false
        },
        attached() {
            if (this.data.show) {
                this.enter();
            }
        },
        methods: {
            observeShow(value) {
                if (value) {
                    this.enter();
                }
                else {
                    this.leave();
                }
            },
            enter() {
                const { duration, name } = this.data;
                const classNames = getClassNames(name);
                const currentDuration = isObj(duration) ? duration.enter : duration;
                this.status = 'enter';
                Promise.resolve()
                    .then(nextTick)
                    .then(() => {
                    this.checkStatus('enter');
                    this.set({
                        inited: true,
                        display: true,
                        classes: classNames.enter,
                        currentDuration
                    });
                })
                    .then(nextTick)
                    .then(() => {
                    this.checkStatus('enter');
                    this.set({
                        classes: classNames['enter-to']
                    });
                })
                    .catch(() => { });
            },
            leave() {
                const { duration, name } = this.data;
                const classNames = getClassNames(name);
                const currentDuration = isObj(duration) ? duration.leave : duration;
                this.status = 'leave';
                Promise.resolve()
                    .then(nextTick)
                    .then(() => {
                    this.checkStatus('leave');
                    this.set({
                        classes: classNames.leave,
                        currentDuration
                    });
                })
                    .then(() => setTimeout(() => this.onTransitionEnd(), currentDuration))
                    .then(nextTick)
                    .then(() => {
                    this.checkStatus('leave');
                    this.set({
                        classes: classNames['leave-to']
                    });
                })
                    .catch(() => { });
            },
            checkStatus(status) {
                if (status !== this.status) {
                    throw new Error(`incongruent status: ${status}`);
                }
            },
            onTransitionEnd() {
                if (!this.data.show) {
                    this.set({ display: false });
                    this.$emit('transitionEnd');
                }
            }
        }
    });
};
export {};
import { VantComponent } from '../common/component';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea({ safeAreaInsetTop: true })],
    classes: ['title-class'],
    props: {
        title: String,
        fixed: Boolean,
        leftText: String,
        rightText: String,
        leftArrow: Boolean,
        border: {
            type: Boolean,
            value: true
        },
        zIndex: {
            type: Number,
            value: 120
        }
    },
    methods: {
        onClickLeft() {
            this.$emit('click-left');
        },
        onClickRight() {
            this.$emit('click-right');
        }
    }
});
export {};
import { VantComponent } from '../common/component';
const FONT_COLOR = '#ed6a0c';
const BG_COLOR = '#fffbe8';
VantComponent({
    props: {
        text: {
            type: String,
            value: ''
        },
        mode: {
            type: String,
            value: ''
        },
        url: {
            type: String,
            value: ''
        },
        openType: {
            type: String,
            value: 'navigate'
        },
        delay: {
            type: Number,
            value: 1
        },
        speed: {
            type: Number,
            value: 50
        },
        scrollable: {
            type: Boolean,
            value: true
        },
        leftIcon: {
            type: String,
            value: ''
        },
        color: {
            type: String,
            value: FONT_COLOR
        },
        backgroundColor: {
            type: String,
            value: BG_COLOR
        },
        wrapable: Boolean
    },
    data: {
        show: true
    },
    watch: {
        text() {
            this.set({}, this.init);
        }
    },
    created() {
        this.resetAnimation = wx.createAnimation({
            duration: 0,
            timingFunction: 'linear'
        });
    },
    destroyed() {
        this.timer && clearTimeout(this.timer);
    },
    methods: {
        init() {
            Promise.all([
                this.getRect('.van-notice-bar__content'),
                this.getRect('.van-notice-bar__wrap')
            ]).then((rects) => {
                const [contentRect, wrapRect] = rects;
                if (contentRect == null ||
                    wrapRect == null ||
                    !contentRect.width ||
                    !wrapRect.width) {
                    return;
                }
                const { speed, scrollable, delay } = this.data;
                if (scrollable && wrapRect.width < contentRect.width) {
                    const duration = (contentRect.width / speed) * 1000;
                    this.wrapWidth = wrapRect.width;
                    this.contentWidth = contentRect.width;
                    this.duration = duration;
                    this.animation = wx.createAnimation({
                        duration,
                        timingFunction: 'linear',
                        delay
                    });
                    this.scroll();
                }
            });
        },
        scroll() {
            this.timer && clearTimeout(this.timer);
            this.timer = null;
            this.set({
                animationData: this.resetAnimation
                    .translateX(this.wrapWidth)
                    .step()
                    .export()
            });
            setTimeout(() => {
                this.set({
                    animationData: this.animation
                        .translateX(-this.contentWidth)
                        .step()
                        .export()
                });
            }, 20);
            this.timer = setTimeout(() => {
                this.scroll();
            }, this.duration);
        },
        onClickIcon() {
            this.timer && clearTimeout(this.timer);
            this.timer = null;
            this.set({ show: false });
        },
        onClick(event) {
            this.$emit('click', event);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { RED } from '../common/color';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea()],
    props: {
        text: String,
        color: {
            type: String,
            value: '#fff'
        },
        backgroundColor: {
            type: String,
            value: RED
        },
        duration: {
            type: Number,
            value: 3000
        },
        zIndex: {
            type: Number,
            value: 110
        }
    },
    methods: {
        show() {
            const { duration } = this.data;
            clearTimeout(this.timer);
            this.set({
                show: true
            });
            if (duration > 0 && duration !== Infinity) {
                this.timer = setTimeout(() => {
                    this.hide();
                }, duration);
            }
        },
        hide() {
            clearTimeout(this.timer);
            this.set({
                show: false
            });
        }
    }
});
interface NotifyOptions {
    text: string;
    color?: string;
    backgroundColor?: string;
    duration?: number;
    selector?: string;
    context?: any;
    safeAreaInsetTop?: boolean;
    zIndex?: number;
}
export default function Notify(options: NotifyOptions | string): void;
export {};
import { isObj } from '../common/utils';
const defaultOptions = {
    selector: '#van-notify',
    duration: 3000
};
function parseOptions(text) {
    return isObj(text) ? text : { text };
}
function getContext() {
    const pages = getCurrentPages();
    return pages[pages.length - 1];
}
export default function Notify(options) {
    options = Object.assign({}, defaultOptions, parseOptions(options));
    const context = options.context || getContext();
    const notify = context.selectComponent(options.selector);
    delete options.context;
    delete options.selector;
    if (notify) {
        notify.set(options);
        notify.show();
    }
    else {
        console.warn('未找到 van-notify 节点，请确认 selector 及 context 是否正确');
    }
}
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        show: Boolean,
        mask: Boolean,
        customStyle: String,
        duration: {
            type: [Number, Object],
            value: 300
        },
        zIndex: {
            type: Number,
            value: 1
        }
    },
    methods: {
        onClick() {
            this.$emit('click');
        },
        // for prevent touchmove
        noop() { }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    classes: ['header-class', 'footer-class'],
    props: {
        desc: String,
        title: String,
        status: String,
        useFooterSlot: Boolean
    }
});
export {};
import { VantComponent } from '../common/component';
import { isObj, range } from '../common/utils';
const DEFAULT_DURATION = 200;
VantComponent({
    classes: ['active-class'],
    props: {
        valueKey: String,
        className: String,
        itemHeight: Number,
        visibleItemCount: Number,
        initialOptions: {
            type: Array,
            value: []
        },
        defaultIndex: {
            type: Number,
            value: 0
        }
    },
    data: {
        startY: 0,
        offset: 0,
        duration: 0,
        startOffset: 0,
        options: [],
        currentIndex: 0
    },
    created() {
        const { defaultIndex, initialOptions } = this.data;
        this.set({
            currentIndex: defaultIndex,
            options: initialOptions
        }).then(() => {
            this.setIndex(defaultIndex);
        });
    },
    computed: {
        count() {
            return this.data.options.length;
        },
        baseOffset() {
            const { data } = this;
            return (data.itemHeight * (data.visibleItemCount - 1)) / 2;
        },
        wrapperStyle() {
            const { data } = this;
            return [
                `transition: ${data.duration}ms`,
                `transform: translate3d(0, ${data.offset + data.baseOffset}px, 0)`,
                `line-height: ${data.itemHeight}px`
            ].join('; ');
        }
    },
    watch: {
        defaultIndex(value) {
            this.setIndex(value);
        }
    },
    methods: {
        onTouchStart(event) {
            this.set({
                startY: event.touches[0].clientY,
                startOffset: this.data.offset,
                duration: 0
            });
        },
        onTouchMove(event) {
            const { data } = this;
            const deltaY = event.touches[0].clientY - data.startY;
            this.set({
                offset: range(data.startOffset + deltaY, -(data.count * data.itemHeight), data.itemHeight)
            });
        },
        onTouchEnd() {
            const { data } = this;
            if (data.offset !== data.startOffset) {
                this.set({
                    duration: DEFAULT_DURATION
                });
                const index = range(Math.round(-data.offset / data.itemHeight), 0, data.count - 1);
                this.setIndex(index, true);
            }
        },
        onClickItem(event) {
            const { index } = event.currentTarget.dataset;
            this.setIndex(index, true);
        },
        adjustIndex(index) {
            const { data } = this;
            index = range(index, 0, data.count);
            for (let i = index; i < data.count; i++) {
                if (!this.isDisabled(data.options[i]))
                    return i;
            }
            for (let i = index - 1; i >= 0; i--) {
                if (!this.isDisabled(data.options[i]))
                    return i;
            }
        },
        isDisabled(option) {
            return isObj(option) && option.disabled;
        },
        getOptionText(option) {
            const { data } = this;
            return isObj(option) && data.valueKey in option
                ? option[data.valueKey]
                : option;
        },
        setIndex(index, userAction) {
            const { data } = this;
            index = this.adjustIndex(index) || 0;
            const offset = -index * data.itemHeight;
            if (index !== data.currentIndex) {
                return this.set({ offset, currentIndex: index }).then(() => {
                    userAction && this.$emit('change', index);
                });
            }
            return this.set({ offset });
        },
        setValue(value) {
            const { options } = this.data;
            for (let i = 0; i < options.length; i++) {
                if (this.getOptionText(options[i]) === value) {
                    return this.setIndex(i);
                }
            }
            return Promise.resolve();
        },
        getValue() {
            const { data } = this;
            return data.options[data.currentIndex];
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { pickerProps } from './shared';
VantComponent({
    classes: ['active-class', 'toolbar-class', 'column-class'],
    props: Object.assign({}, pickerProps, { valueKey: {
            type: String,
            value: 'text'
        }, defaultIndex: {
            type: Number,
            value: 0
        }, columns: {
            type: Array,
            value: [],
            observer(columns = []) {
                this.simple = columns.length && !columns[0].values;
                this.children = this.selectAllComponents('.van-picker__column');
                if (Array.isArray(this.children) && this.children.length) {
                    this.setColumns().catch(() => { });
                }
            }
        } }),
    beforeCreate() {
        this.children = [];
    },
    methods: {
        noop() { },
        setColumns() {
            const { data } = this;
            const columns = this.simple ? [{ values: data.columns }] : data.columns;
            const stack = columns.map((column, index) => this.setColumnValues(index, column.values));
            return Promise.all(stack);
        },
        emit(event) {
            const { type } = event.currentTarget.dataset;
            if (this.simple) {
                this.$emit(type, {
                    value: this.getColumnValue(0),
                    index: this.getColumnIndex(0)
                });
            }
            else {
                this.$emit(type, {
                    value: this.getValues(),
                    index: this.getIndexes()
                });
            }
        },
        onChange(event) {
            if (this.simple) {
                this.$emit('change', {
                    picker: this,
                    value: this.getColumnValue(0),
                    index: this.getColumnIndex(0)
                });
            }
            else {
                this.$emit('change', {
                    picker: this,
                    value: this.getValues(),
                    index: event.currentTarget.dataset.index
                });
            }
        },
        // get column instance by index
        getColumn(index) {
            return this.children[index];
        },
        // get column value by index
        getColumnValue(index) {
            const column = this.getColumn(index);
            return column && column.getValue();
        },
        // set column value by index
        setColumnValue(index, value) {
            const column = this.getColumn(index);
            if (column == null) {
                return Promise.reject(new Error('setColumnValue: 对应列不存在'));
            }
            return column.setValue(value);
        },
        // get column option index by column index
        getColumnIndex(columnIndex) {
            return (this.getColumn(columnIndex) || {}).data.currentIndex;
        },
        // set column option index by column index
        setColumnIndex(columnIndex, optionIndex) {
            const column = this.getColumn(columnIndex);
            if (column == null) {
                return Promise.reject(new Error('setColumnIndex: 对应列不存在'));
            }
            return column.setIndex(optionIndex);
        },
        // get options of column by index
        getColumnValues(index) {
            return (this.children[index] || {}).data.options;
        },
        // set options of column by index
        setColumnValues(index, options, needReset = true) {
            const column = this.children[index];
            if (column == null) {
                return Promise.reject(new Error('setColumnValues: 对应列不存在'));
            }
            const isSame = JSON.stringify(column.data.options) === JSON.stringify(options);
            if (isSame) {
                return Promise.resolve();
            }
            return column.set({ options }).then(() => {
                if (needReset) {
                    column.setIndex(0);
                }
            });
        },
        // get values of all columns
        getValues() {
            return this.children.map((child) => child.getValue());
        },
        // set values of all columns
        setValues(values) {
            const stack = values.map((value, index) => this.setColumnValue(index, value));
            return Promise.all(stack);
        },
        // get indexes of all columns
        getIndexes() {
            return this.children.map((child) => child.data.currentIndex);
        },
        // set indexes of all columns
        setIndexes(indexes) {
            const stack = indexes.map((optionIndex, columnIndex) => this.setColumnIndex(columnIndex, optionIndex));
            return Promise.all(stack);
        }
    }
});
export declare const pickerProps: {
    title: StringConstructor;
    loading: BooleanConstructor;
    showToolbar: BooleanConstructor;
    cancelButtonText: {
        type: StringConstructor;
        value: string;
    };
    confirmButtonText: {
        type: StringConstructor;
        value: string;
    };
    visibleItemCount: {
        type: NumberConstructor;
        value: number;
    };
    itemHeight: {
        type: NumberConstructor;
        value: number;
    };
};
export const pickerProps = {
    title: String,
    loading: Boolean,
    showToolbar: Boolean,
    cancelButtonText: {
        type: String,
        value: '取消'
    },
    confirmButtonText: {
        type: String,
        value: '确认'
    },
    visibleItemCount: {
        type: Number,
        value: 5
    },
    itemHeight: {
        type: Number,
        value: 44
    }
};
export {};
import { VantComponent } from '../common/component';
import { transition } from '../mixins/transition';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    classes: [
        'enter-class',
        'enter-active-class',
        'enter-to-class',
        'leave-class',
        'leave-active-class',
        'leave-to-class'
    ],
    mixins: [transition(false), safeArea()],
    props: {
        transition: {
            type: String,
            observer: 'observeClass'
        },
        customStyle: String,
        overlayStyle: String,
        zIndex: {
            type: Number,
            value: 100
        },
        overlay: {
            type: Boolean,
            value: true
        },
        closeOnClickOverlay: {
            type: Boolean,
            value: true
        },
        position: {
            type: String,
            value: 'center',
            observer: 'observeClass'
        }
    },
    created() {
        this.observeClass();
    },
    methods: {
        onClickOverlay() {
            this.$emit('click-overlay');
            if (this.data.closeOnClickOverlay) {
                this.$emit('close');
            }
        },
        observeClass() {
            const { transition, position } = this.data;
            const updateData = {
                name: transition || position
            };
            if (transition === 'none') {
                updateData.duration = 0;
            }
            this.set(updateData);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { BLUE } from '../common/color';
VantComponent({
    props: {
        inactive: Boolean,
        percentage: Number,
        pivotText: String,
        pivotColor: String,
        showPivot: {
            type: Boolean,
            value: true
        },
        color: {
            type: String,
            value: BLUE
        },
        textColor: {
            type: String,
            value: '#fff'
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    relation: {
        name: 'radio',
        type: 'descendant',
        linked(target) {
            this.children = this.children || [];
            this.children.push(target);
            this.updateChild(target);
        },
        unlinked(target) {
            this.children = this.children.filter((child) => child !== target);
        }
    },
    props: {
        value: {
            type: null,
            observer: 'updateChildren'
        },
        disabled: {
            type: Boolean,
            observer: 'updateChildren'
        }
    },
    methods: {
        updateChildren() {
            (this.children || []).forEach((child) => this.updateChild(child));
        },
        updateChild(child) {
            const { value, disabled } = this.data;
            child.set({
                value,
                disabled: disabled || child.data.disabled
            });
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    relation: {
        name: 'radio-group',
        type: 'ancestor',
        linked(target) {
            this.parent = target;
        },
        unlinked() {
            this.parent = null;
        }
    },
    classes: ['icon-class', 'label-class'],
    props: {
        value: null,
        disabled: Boolean,
        useIconSlot: Boolean,
        checkedColor: String,
        labelPosition: {
            type: String,
            value: 'right'
        },
        labelDisabled: Boolean,
        shape: {
            type: String,
            value: 'round'
        }
    },
    methods: {
        emitChange(value) {
            const instance = this.parent || this;
            instance.$emit('input', value);
            instance.$emit('change', value);
        },
        onChange(event) {
            console.log(event);
            this.emitChange(this.data.name);
        },
        onClickLabel() {
            const { disabled, labelDisabled, name } = this.data;
            if (!disabled && !labelDisabled) {
                this.emitChange(name);
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    classes: ['icon-class'],
    props: {
        value: Number,
        readonly: Boolean,
        disabled: Boolean,
        allowHalf: Boolean,
        size: {
            type: Number,
            value: 20
        },
        icon: {
            type: String,
            value: 'star'
        },
        voidIcon: {
            type: String,
            value: 'star-o'
        },
        color: {
            type: String,
            value: '#ffd21e'
        },
        voidColor: {
            type: String,
            value: '#c7c7c7'
        },
        disabledColor: {
            type: String,
            value: '#bdbdbd'
        },
        count: {
            type: Number,
            value: 5
        }
    },
    data: {
        innerValue: 0
    },
    watch: {
        value(value) {
            if (value !== this.data.innerValue) {
                this.set({ innerValue: value });
            }
        }
    },
    methods: {
        onSelect(event) {
            const { data } = this;
            const { score } = event.currentTarget.dataset;
            if (!data.disabled && !data.readonly) {
                this.set({ innerValue: score + 1 });
                this.$emit('input', score + 1);
                this.$emit('change', score + 1);
            }
        },
        onTouchMove(event) {
            const { clientX, clientY } = event.touches[0];
            this.getRect('.van-rate__icon', true).then((list) => {
                const target = list
                    .sort(item => item.right - item.left)
                    .find(item => clientX >= item.left &&
                    clientX <= item.right &&
                    clientY >= item.top &&
                    clientY <= item.bottom);
                if (target != null) {
                    this.onSelect(Object.assign({}, event, { currentTarget: target }));
                }
            });
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        name: 'col',
        type: 'descendant',
        linked(target) {
            if (this.data.gutter) {
                target.setGutter(this.data.gutter);
            }
        }
    },
    props: {
        gutter: Number
    },
    watch: {
        gutter: 'setGutter'
    },
    mounted() {
        if (this.data.gutter) {
            this.setGutter();
        }
    },
    methods: {
        setGutter() {
            const { gutter } = this.data;
            const margin = `-${Number(gutter) / 2}px`;
            const style = gutter
                ? `margin-right: ${margin}; margin-left: ${margin};`
                : '';
            this.set({ style });
            this.getRelationNodes('../col/index').forEach(col => {
                col.setGutter(this.data.gutter);
            });
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    classes: ['field-class', 'input-class', 'cancel-class'],
    props: {
        label: String,
        focus: Boolean,
        error: Boolean,
        disabled: Boolean,
        readonly: Boolean,
        inputAlign: String,
        showAction: Boolean,
        useActionSlot: Boolean,
        placeholder: String,
        placeholderStyle: String,
        background: {
            type: String,
            value: '#ffffff'
        },
        maxlength: {
            type: Number,
            value: -1
        },
        shape: {
            type: String,
            value: 'square'
        },
        clearable: {
            type: Boolean,
            value: true
        }
    },
    methods: {
        onChange(event) {
            this.set({ value: event.detail });
            this.$emit('change', event.detail);
        },
        onCancel() {
            /**
             * 修复修改输入框值时，输入框失焦和赋值同时触发，赋值失效
             * // https://github.com/youzan/vant-weapp/issues/1768
             */
            setTimeout(() => {
                this.set({ value: '' });
                this.$emit('cancel');
                this.$emit('change', '');
            }, 200);
        },
        onSearch() {
            this.$emit('search', this.data.value);
        },
        onFocus() {
            this.$emit('focus');
        },
        onBlur() {
            this.$emit('blur');
        },
        onClear() {
            this.$emit('clear');
        },
    }
});
export {};
import { VantComponent } from '../common/component';
import { touch } from '../mixins/touch';
VantComponent({
    mixins: [touch],
    props: {
        disabled: Boolean,
        useButtonSlot: Boolean,
        activeColor: String,
        inactiveColor: String,
        max: {
            type: Number,
            value: 100
        },
        min: {
            type: Number,
            value: 0
        },
        step: {
            type: Number,
            value: 1
        },
        value: {
            type: Number,
            value: 0
        },
        barHeight: {
            type: String,
            value: '2px'
        }
    },
    watch: {
        value(value) {
            this.updateValue(value, false);
        }
    },
    created() {
        this.updateValue(this.data.value);
    },
    methods: {
        onTouchStart(event) {
            if (this.data.disabled)
                return;
            this.touchStart(event);
            this.startValue = this.format(this.data.value);
        },
        onTouchMove(event) {
            if (this.data.disabled)
                return;
            this.touchMove(event);
            this.getRect('.van-slider').then((rect) => {
                const diff = this.deltaX / rect.width * 100;
                this.newValue = this.startValue + diff;
                this.updateValue(this.newValue, false, true);
            });
        },
        onTouchEnd() {
            if (this.data.disabled)
                return;
            this.updateValue(this.newValue, true);
        },
        onClick(event) {
            if (this.data.disabled)
                return;
            this.getRect('.van-slider').then((rect) => {
                const value = (event.detail.x - rect.left) / rect.width * 100;
                this.updateValue(value, true);
            });
        },
        updateValue(value, end, drag) {
            value = this.format(value);
            this.set({
                value,
                barStyle: `width: ${value}%; height: ${this.data.barHeight};`
            });
            if (drag) {
                this.$emit('drag', { value });
            }
            if (end) {
                this.$emit('change', value);
            }
        },
        format(value) {
            const { max, min, step } = this.data;
            return Math.round(Math.max(min, Math.min(value, max)) / step) * step;
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    classes: [
        'input-class',
        'plus-class',
        'minus-class'
    ],
    props: {
        value: null,
        integer: Boolean,
        disabled: Boolean,
        inputWidth: String,
        asyncChange: Boolean,
        disableInput: Boolean,
        min: {
            type: null,
            value: 1
        },
        max: {
            type: null,
            value: Number.MAX_SAFE_INTEGER
        },
        step: {
            type: null,
            value: 1
        },
        showPlus: {
            type: Boolean,
            value: true
        },
        showMinus: {
            type: Boolean,
            value: true
        }
    },
    computed: {
        minusDisabled() {
            return this.data.disabled || this.data.value <= this.data.min;
        },
        plusDisabled() {
            return this.data.disabled || this.data.value >= this.data.max;
        }
    },
    watch: {
        value(value) {
            if (value === '') {
                return;
            }
            const newValue = this.range(value);
            if (typeof newValue === 'number' && +this.data.value !== newValue) {
                this.set({ value: newValue });
            }
        }
    },
    data: {
        focus: false
    },
    created() {
        this.set({
            value: this.range(this.data.value)
        });
    },
    methods: {
        onFocus(event) {
            this.$emit('focus', event.detail);
        },
        onBlur(event) {
            const value = this.range(this.data.value);
            this.triggerInput(value);
            this.$emit('blur', event.detail);
        },
        // limit value range
        range(value) {
            value = String(value).replace(/[^0-9.-]/g, '');
            return Math.max(Math.min(this.data.max, value), this.data.min);
        },
        onInput(event) {
            const { value = '' } = event.detail || {};
            this.triggerInput(value);
        },
        onChange(type) {
            if (this.data[`${type}Disabled`]) {
                this.$emit('overlimit', type);
                return;
            }
            const diff = type === 'minus' ? -this.data.step : +this.data.step;
            const value = Math.round((+this.data.value + diff) * 100) / 100;
            this.triggerInput(this.range(value));
            this.$emit(type);
        },
        onMinus() {
            this.onChange('minus');
        },
        onPlus() {
            this.onChange('plus');
        },
        triggerInput(value) {
            this.set({
                value: this.data.asyncChange ? this.data.value : value
            });
            this.$emit('change', value);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { GREEN } from '../common/color';
VantComponent({
    props: {
        icon: String,
        steps: Array,
        active: Number,
        direction: {
            type: String,
            value: 'horizontal'
        },
        activeColor: {
            type: String,
            value: GREEN
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea()],
    classes: [
        'bar-class',
        'price-class',
        'button-class'
    ],
    props: {
        tip: {
            type: null,
            observer: 'updateTip'
        },
        tipIcon: String,
        type: Number,
        price: {
            type: null,
            observer: 'updatePrice'
        },
        label: String,
        loading: Boolean,
        disabled: Boolean,
        buttonText: String,
        currency: {
            type: String,
            value: '¥'
        },
        buttonType: {
            type: String,
            value: 'danger'
        },
        decimalLength: {
            type: Number,
            value: 2,
            observer: 'updatePrice'
        },
        suffixLabel: String
    },
    methods: {
        updatePrice() {
            const { price, decimalLength } = this.data;
            this.set({
                hasPrice: typeof price === 'number',
                priceStr: (price / 100).toFixed(decimalLength)
            });
        },
        updateTip() {
            this.set({ hasTip: typeof this.data.tip === 'string' });
        },
        onSubmit(event) {
            this.$emit('submit', event.detail);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { touch } from '../mixins/touch';
const THRESHOLD = 0.3;
VantComponent({
    props: {
        disabled: Boolean,
        leftWidth: {
            type: Number,
            value: 0
        },
        rightWidth: {
            type: Number,
            value: 0
        },
        asyncClose: Boolean
    },
    mixins: [touch],
    data: {
        catchMove: false
    },
    created() {
        this.offset = 0;
    },
    methods: {
        open(position) {
            const { leftWidth, rightWidth } = this.data;
            const offset = position === 'left' ? leftWidth : -rightWidth;
            this.swipeMove(offset);
        },
        close() {
            this.swipeMove(0);
        },
        swipeMove(offset = 0) {
            this.offset = offset;
            const transform = `translate3d(${offset}px, 0, 0)`;
            const transition = this.draging
                ? 'none'
                : '.6s cubic-bezier(0.18, 0.89, 0.32, 1)';
            this.set({
                wrapperStyle: `
        -webkit-transform: ${transform};
        -webkit-transition: ${transition};
        transform: ${transform};
        transition: ${transition};
      `
            });
        },
        swipeLeaveTransition() {
            const { leftWidth, rightWidth } = this.data;
            const { offset } = this;
            if (rightWidth > 0 && -offset > rightWidth * THRESHOLD) {
                this.open('right');
            }
            else if (leftWidth > 0 && offset > leftWidth * THRESHOLD) {
                this.open('left');
            }
            else {
                this.swipeMove(0);
            }
            this.set({ catchMove: false });
        },
        startDrag(event) {
            if (this.data.disabled) {
                return;
            }
            this.draging = true;
            this.startOffset = this.offset;
            this.firstDirection = '';
            this.touchStart(event);
        },
        noop() { },
        onDrag(event) {
            if (this.data.disabled) {
                return;
            }
            this.touchMove(event);
            if (!this.firstDirection) {
                this.firstDirection = this.direction;
                this.set({ catchMove: this.firstDirection === 'horizontal' });
            }
            if (this.firstDirection === 'vertical') {
                return;
            }
            const { leftWidth, rightWidth } = this.data;
            const offset = this.startOffset + this.deltaX;
            if ((rightWidth > 0 && -offset > rightWidth) ||
                (leftWidth > 0 && offset > leftWidth)) {
                return;
            }
            this.swipeMove(offset);
        },
        endDrag() {
            if (this.data.disabled) {
                return;
            }
            this.draging = false;
            this.swipeLeaveTransition();
        },
        onClick(event) {
            const { key: position = 'outside' } = event.currentTarget.dataset;
            this.$emit('click', position);
            if (!this.offset) {
                return;
            }
            if (this.data.asyncClose) {
                this.$emit('close', { position, instance: this });
            }
            else {
                this.swipeMove(0);
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    props: {
        value: null,
        icon: String,
        title: String,
        label: String,
        border: Boolean,
        checked: Boolean,
        loading: Boolean,
        disabled: Boolean,
        activeColor: String,
        inactiveColor: String,
        useLabelSlot: Boolean,
        size: {
            type: String,
            value: '24px'
        },
        activeValue: {
            type: null,
            value: true
        },
        inactiveValue: {
            type: null,
            value: false
        }
    },
    watch: {
        checked(value) {
            this.set({ value });
        }
    },
    created() {
        this.set({ value: this.data.checked });
    },
    methods: {
        onChange(event) {
            this.$emit('change', event.detail);
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    field: true,
    classes: ['node-class'],
    props: {
        checked: null,
        loading: Boolean,
        disabled: Boolean,
        activeColor: String,
        inactiveColor: String,
        size: {
            type: String,
            value: '30px'
        },
        activeValue: {
            type: null,
            value: true
        },
        inactiveValue: {
            type: null,
            value: false
        }
    },
    watch: {
        checked(value) {
            this.set({ value });
        }
    },
    created() {
        this.set({ value: this.data.checked });
    },
    methods: {
        onClick() {
            const { activeValue, inactiveValue } = this.data;
            if (!this.data.disabled && !this.data.loading) {
                const checked = this.data.checked === activeValue;
                const value = checked ? inactiveValue : activeValue;
                this.$emit('input', value);
                this.$emit('change', value);
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    relation: {
        name: 'tabs',
        type: 'ancestor'
    },
    props: {
        dot: Boolean,
        info: null,
        title: String,
        disabled: Boolean,
        titleStyle: String
    },
    data: {
        width: null,
        inited: false,
        active: false,
        animated: false
    },
    watch: {
        title: 'update',
        disabled: 'update',
        dot: 'update',
        info: 'update',
        titleStyle: 'update'
    },
    methods: {
        update() {
            const parent = this.getRelationNodes('../tabs/index')[0];
            if (parent) {
                parent.updateTabs();
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        info: null,
        icon: String,
        dot: Boolean,
        name: {
            type: [String, Number]
        }
    },
    relation: {
        name: 'tabbar',
        type: 'ancestor'
    },
    data: {
        active: false
    },
    methods: {
        onClick() {
            if (this.parent) {
                this.parent.onChange(this);
            }
            this.$emit('click');
        },
        updateFromParent() {
            const { parent } = this;
            if (!parent) {
                return;
            }
            const index = parent.children.indexOf(this);
            const parentData = parent.data;
            const { data } = this;
            const active = (data.name || index) === parentData.active;
            const patch = {};
            if (active !== data.active) {
                patch.active = active;
            }
            if (parentData.activeColor !== data.activeColor) {
                patch.activeColor = parentData.activeColor;
            }
            if (parentData.inactiveColor !== data.inactiveColor) {
                patch.inactiveColor = parentData.inactiveColor;
            }
            return Object.keys(patch).length > 0
                ? this.set(patch)
                : Promise.resolve();
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { safeArea } from '../mixins/safe-area';
VantComponent({
    mixins: [safeArea()],
    relation: {
        name: 'tabbar-item',
        type: 'descendant',
        linked(target) {
            this.children.push(target);
            target.parent = this;
            target.updateFromParent();
        },
        unlinked(target) {
            this.children = this.children.filter((item) => item !== target);
            this.updateChildren();
        }
    },
    props: {
        active: {
            type: [Number, String],
            observer: 'updateChildren'
        },
        activeColor: {
            type: String,
            observer: 'updateChildren'
        },
        inactiveColor: {
            type: String,
            observer: 'updateChildren'
        },
        fixed: {
            type: Boolean,
            value: true
        },
        border: {
            type: Boolean,
            value: true
        },
        zIndex: {
            type: Number,
            value: 1
        }
    },
    beforeCreate() {
        this.children = [];
    },
    methods: {
        updateChildren() {
            const { children } = this;
            if (!Array.isArray(children) || !children.length) {
                return Promise.resolve();
            }
            return Promise.all(children.map((child) => child.updateFromParent()));
        },
        onChange(child) {
            const index = this.children.indexOf(child);
            const active = child.data.name || index;
            if (active !== this.data.active) {
                this.$emit('change', active);
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { touch } from '../mixins/touch';
import { nextTick } from '../common/utils';
VantComponent({
    mixins: [touch],
    classes: ['nav-class', 'tab-class', 'tab-active-class', 'line-class'],
    relation: {
        name: 'tab',
        type: 'descendant',
        linked(child) {
            this.child.push(child);
            this.updateTabs(this.data.tabs.concat(child.data));
        },
        unlinked(child) {
            const index = this.child.indexOf(child);
            const { tabs } = this.data;
            tabs.splice(index, 1);
            this.child.splice(index, 1);
            this.updateTabs(tabs);
        }
    },
    props: {
        color: String,
        sticky: Boolean,
        animated: Boolean,
        swipeable: Boolean,
        lineWidth: {
            type: Number,
            value: -1
        },
        lineHeight: {
            type: Number,
            value: -1
        },
        active: {
            type: Number,
            value: 0
        },
        type: {
            type: String,
            value: 'line'
        },
        border: {
            type: Boolean,
            value: true
        },
        duration: {
            type: Number,
            value: 0.3
        },
        zIndex: {
            type: Number,
            value: 1
        },
        swipeThreshold: {
            type: Number,
            value: 4
        },
        offsetTop: {
            type: Number,
            value: 0
        }
    },
    data: {
        tabs: [],
        lineStyle: '',
        scrollLeft: 0,
        scrollable: false,
        trackStyle: '',
        wrapStyle: '',
        position: ''
    },
    watch: {
        swipeThreshold() {
            this.set({
                scrollable: this.child.length > this.data.swipeThreshold
            });
        },
        color: 'setLine',
        lineWidth: 'setLine',
        lineHeight: 'setLine',
        active: 'setActiveTab',
        animated: 'setTrack',
        offsetTop: 'setWrapStyle'
    },
    beforeCreate() {
        this.child = [];
    },
    mounted() {
        this.setLine(true);
        this.setTrack();
        this.scrollIntoView();
        this.getRect('.van-tabs__wrap').then((rect) => {
            this.navHeight = rect.height;
            this.observerContentScroll();
        });
    },
    destroyed() {
        // @ts-ignore
        this.createIntersectionObserver().disconnect();
    },
    methods: {
        updateTabs(tabs) {
            tabs = tabs || this.data.tabs;
            this.set({
                tabs,
                scrollable: tabs.length > this.data.swipeThreshold
            });
            this.setActiveTab();
        },
        trigger(eventName, index) {
            this.$emit(eventName, {
                index,
                title: this.data.tabs[index].title
            });
        },
        onTap(event) {
            const { index } = event.currentTarget.dataset;
            if (this.data.tabs[index].disabled) {
                this.trigger('disabled', index);
            }
            else {
                this.trigger('click', index);
                this.setActive(index);
            }
        },
        setActive(active) {
            if (active !== this.data.active) {
                this.trigger('change', active);
                this.set({ active });
                this.setActiveTab();
            }
        },
        setLine(skipTransition) {
            if (this.data.type !== 'line') {
                return;
            }
            const { color, active, duration, lineWidth, lineHeight } = this.data;
            this.getRect('.van-tab', true).then((rects) => {
                const rect = rects[active];
                const width = lineWidth !== -1 ? lineWidth : rect.width / 2;
                const height = lineHeight !== -1 ? `height: ${lineHeight}px;` : '';
                let left = rects
                    .slice(0, active)
                    .reduce((prev, curr) => prev + curr.width, 0);
                left += (rect.width - width) / 2;
                const transition = skipTransition
                    ? ''
                    : `transition-duration: ${duration}s; -webkit-transition-duration: ${duration}s;`;
                this.set({
                    lineStyle: `
            ${height}
            width: ${width}px;
            background-color: ${color};
            -webkit-transform: translateX(${left}px);
            transform: translateX(${left}px);
            ${transition}
          `
                });
            });
        },
        setTrack() {
            const { animated, active, duration } = this.data;
            if (!animated)
                return '';
            this.getRect('.van-tabs__content').then((rect) => {
                const { width } = rect;
                this.set({
                    trackStyle: `
            width: ${width * this.child.length}px;
            left: ${-1 * active * width}px;
            transition: left ${duration}s;
            display: -webkit-box;
            display: flex;
          `
                });
                const props = { width, animated };
                this.child.forEach((item) => {
                    item.set(props);
                });
            });
        },
        setActiveTab() {
            this.child.forEach((item, index) => {
                const data = {
                    active: index === this.data.active
                };
                if (data.active) {
                    data.inited = true;
                }
                if (data.active !== item.data.active) {
                    item.set(data);
                }
            });
            nextTick(() => {
                this.setLine();
                this.setTrack();
                this.scrollIntoView();
            });
        },
        // scroll active tab into view
        scrollIntoView() {
            const { active, scrollable } = this.data;
            if (!scrollable) {
                return;
            }
            Promise.all([
                this.getRect('.van-tab', true),
                this.getRect('.van-tabs__nav')
            ]).then(([tabRects, navRect]) => {
                const tabRect = tabRects[active];
                const offsetLeft = tabRects
                    .slice(0, active)
                    .reduce((prev, curr) => prev + curr.width, 0);
                this.set({
                    scrollLeft: offsetLeft - (navRect.width - tabRect.width) / 2
                });
            });
        },
        onTouchStart(event) {
            if (!this.data.swipeable)
                return;
            this.touchStart(event);
        },
        onTouchMove(event) {
            if (!this.data.swipeable)
                return;
            this.touchMove(event);
        },
        // watch swipe touch end
        onTouchEnd() {
            if (!this.data.swipeable)
                return;
            const { active, tabs } = this.data;
            const { direction, deltaX, offsetX } = this;
            const minSwipeDistance = 50;
            if (direction === 'horizontal' && offsetX >= minSwipeDistance) {
                if (deltaX > 0 && active !== 0) {
                    this.setActive(active - 1);
                }
                else if (deltaX < 0 && active !== tabs.length - 1) {
                    this.setActive(active + 1);
                }
            }
        },
        setWrapStyle() {
            const { offsetTop, position } = this.data;
            let wrapStyle;
            switch (position) {
                case 'top':
                    wrapStyle = `
            top: ${offsetTop}px;
            position: fixed;
          `;
                    break;
                case 'bottom':
                    wrapStyle = `
            top: auto;
            bottom: 0;
          `;
                    break;
                default:
                    wrapStyle = '';
            }
            // cut down `set`
            if (wrapStyle === this.data.wrapStyle)
                return;
            this.set({ wrapStyle });
        },
        observerContentScroll() {
            if (!this.data.sticky) {
                return;
            }
            const { offsetTop } = this.data;
            const { windowHeight } = wx.getSystemInfoSync();
            // @ts-ignore
            this.createIntersectionObserver().disconnect();
            // @ts-ignore
            this.createIntersectionObserver()
                .relativeToViewport({ top: -(this.navHeight + offsetTop) })
                .observe('.van-tabs', (res) => {
                const { top } = res.boundingClientRect;
                if (top > offsetTop) {
                    return;
                }
                const position = res.intersectionRatio > 0 ? 'top' : 'bottom';
                this.$emit('scroll', {
                    scrollTop: top + offsetTop,
                    isFixed: position === 'top'
                });
                this.setPosition(position);
            });
            // @ts-ignore
            this.createIntersectionObserver()
                .relativeToViewport({ bottom: -(windowHeight - 1 - offsetTop) })
                .observe('.van-tabs', (res) => {
                const { top, bottom } = res.boundingClientRect;
                if (bottom < this.navHeight) {
                    return;
                }
                const position = res.intersectionRatio > 0 ? 'top' : '';
                this.$emit('scroll', {
                    scrollTop: top + offsetTop,
                    isFixed: position === 'top'
                });
                this.setPosition(position);
            });
        },
        setPosition(position) {
            if (position !== this.data.position) {
                this.set({ position }).then(() => {
                    this.setWrapStyle();
                });
            }
        }
    }
});
export {};
import { VantComponent } from '../common/component';
import { RED, BLUE, GREEN, ORANGE } from '../common/color';
const DEFAULT_COLOR = '#999';
const COLOR_MAP = {
    danger: RED,
    primary: BLUE,
    success: GREEN,
    warning: ORANGE
};
VantComponent({
    props: {
        size: String,
        type: String,
        mark: Boolean,
        color: String,
        plain: Boolean,
        round: Boolean,
        textColor: String
    },
    computed: {
        style() {
            const color = this.data.color || COLOR_MAP[this.data.type] || DEFAULT_COLOR;
            const key = this.data.plain ? 'color' : 'background-color';
            const style = { [key]: color };
            if (this.data.textColor) {
                style.color = this.data.textColor;
            }
            return Object.keys(style).map(key => `${key}: ${style[key]}`).join(';');
        }
    }
});
export {};
import { VantComponent } from '../common/component';
VantComponent({
    props: {
        show: Boolean,
        mask: Boolean,
        message: String,
        forbidClick: Boolean,
        zIndex: {
            type: Number,
            value: 1000
        },
        type: {
            type: String,
            value: 'text'
        },
        loadingType: {
            type: String,
            value: 'circular'
        },
        position: {
            type: String,
            value: 'middle'
        }
    },
    methods: {
        // for prevent touchmove
        noop() { }
    }
});
/// <reference types="miniprogram-api-typings" />
declare type ToastMessage = string | number;
interface ToastOptions {
    show?: boolean;
    type?: string;
    mask?: boolean;
    zIndex?: number;
    context?: WechatMiniprogram.Component.TrivialInstance | WechatMiniprogram.Page.TrivialInstance;
    position?: string;
    duration?: number;
    selector?: string;
    forbidClick?: boolean;
    loadingType?: string;
    message?: ToastMessage;
    onClose?: () => void;
}
declare function Toast(toastOptions: ToastOptions | ToastMessage): WechatMiniprogram.Component.TrivialInstance;
declare namespace Toast {
    var loading: (options: string | number | ToastOptions) => WechatMiniprogram.Component.Instance<Record<string, any>, Record<string, any>, Record<string, any>>;
    var success: (options: string | number | ToastOptions) => WechatMiniprogram.Component.Instance<Record<string, any>, Record<string, any>, Record<string, any>>;
    var fail: (options: string | number | ToastOptions) => WechatMiniprogram.Component.Instance<Record<string, any>, Record<string, any>, Record<string, any>>;
    var clear: () => void;
    var setDefaultOptions: (options: ToastOptions) => void;
    var resetDefaultOptions: () => void;
}
export default Toast;
import { isObj } from '../common/utils';
const defaultOptions = {
    type: 'text',
    mask: false,
    message: '',
    show: true,
    zIndex: 1000,
    duration: 3000,
    position: 'middle',
    forbidClick: false,
    loadingType: 'circular',
    selector: '#van-toast'
};
let queue = [];
let currentOptions = Object.assign({}, defaultOptions);
function parseOptions(message) {
    return isObj(message) ? message : { message };
}
function getContext() {
    const pages = getCurrentPages();
    return pages[pages.length - 1];
}
function Toast(toastOptions) {
    const options = Object.assign({}, currentOptions, parseOptions(toastOptions));
    const context = options.context || getContext();
    const toast = context.selectComponent(options.selector);
    if (!toast) {
        console.warn('未找到 van-toast 节点，请确认 selector 及 context 是否正确');
        return;
    }
    delete options.context;
    delete options.selector;
    toast.clear = () => {
        toast.set({ show: false });
        if (options.onClose) {
            options.onClose();
        }
    };
    queue.push(toast);
    toast.set(options);
    clearTimeout(toast.timer);
    if (options.duration > 0) {
        toast.timer = setTimeout(() => {
            toast.clear();
            queue = queue.filter(item => item !== toast);
        }, options.duration);
    }
    return toast;
}
const createMethod = (type) => (options) => Toast(Object.assign({ type }, parseOptions(options)));
Toast.loading = createMethod('loading');
Toast.success = createMethod('success');
Toast.fail = createMethod('fail');
Toast.clear = () => {
    queue.forEach(toast => {
        toast.clear();
    });
    queue = [];
};
Toast.setDefaultOptions = (options) => {
    Object.assign(currentOptions, options);
};
Toast.resetDefaultOptions = () => {
    currentOptions = Object.assign({}, defaultOptions);
};
export default Toast;
export {};
import { VantComponent } from '../common/component';
import { transition } from '../mixins/transition';
VantComponent({
    classes: [
        'enter-class',
        'enter-active-class',
        'enter-to-class',
        'leave-class',
        'leave-active-class',
        'leave-to-class'
    ],
    mixins: [transition(true)]
});
export {};
import { VantComponent } from '../common/component';
const ITEM_HEIGHT = 44;
VantComponent({
    classes: [
        'main-item-class',
        'content-item-class',
        'main-active-class',
        'content-active-class',
        'main-disabled-class',
        'content-disabled-class'
    ],
    props: {
        items: Array,
        mainActiveIndex: {
            type: Number,
            value: 0
        },
        activeId: {
            type: [Number, String, Array]
        },
        maxHeight: {
            type: Number,
            value: 300
        }
    },
    data: {
        subItems: [],
        mainHeight: 0,
        itemHeight: 0
    },
    watch: {
        items() {
            this.updateSubItems().then(() => {
                this.updateMainHeight();
            });
        },
        maxHeight() {
            this.updateItemHeight(this.data.subItems);
            this.updateMainHeight();
        },
        mainActiveIndex: 'updateSubItems'
    },
    methods: {
        // 当一个子项被选择时
        onSelectItem(event) {
            const { item } = event.currentTarget.dataset;
            if (!item.disabled) {
                this.$emit('click-item', item);
            }
        },
        // 当一个导航被点击时
        onClickNav(event) {
            const { index } = event.currentTarget.dataset;
            const item = this.data.items[index];
            if (!item.disabled) {
                this.$emit('click-nav', { index });
            }
        },
        // 更新子项列表
        updateSubItems() {
            const { items, mainActiveIndex } = this.data;
            const { children = [] } = items[mainActiveIndex] || {};
            this.updateItemHeight(children);
            return this.set({ subItems: children });
        },
        // 更新组件整体高度，根据最大高度和当前组件需要展示的高度来决定
        updateMainHeight() {
            const { items = [], subItems = [] } = this.data;
            const maxHeight = Math.max(items.length * ITEM_HEIGHT, subItems.length * ITEM_HEIGHT);
            this.set({ mainHeight: Math.min(maxHeight, this.data.maxHeight) });
        },
        // 更新子项列表高度，根据可展示的最大高度和当前子项列表的高度决定
        updateItemHeight(subItems) {
            const itemHeight = Math.min(subItems.length * ITEM_HEIGHT, this.data.maxHeight);
            return this.set({ itemHeight });
        }
    }
});
 
let timer
    /**
     * 页面的初始数据
     */
    data: {
        hotkeys: [],
        query: "",
        offset: -1,
        songItem: []
    },
    debounce(e) {
        if (timer) {
            clearTimeout(timer)
        }
        timer = setTimeout(() => { this.searchbox(e) }, 300)
    },
    api(url, func) {

        let host = 'http://www.china-4s.com/'
        wx.request({
            url: host + url,
            success: func
        })
    },
    searchbox(e) {
        this.setData({
            query: e.detail,
            offset: 0
        })
        console.log(this.data.query);
        if (this.data.query) { this.search(this.data.query, this.data.offset * 30) }

    },
    search(query, offset) {
        wx.request({
            url: 'http://localhost:3000/search',
            data: {
                keywords: query,
                offset: offset
            },
            success: res => {
                console.log(res.data.result.songs);
                this.setData({ songItem: res.data.result.songs })
            }
        })
    },
    /**
     * 生命周期函数--监听页面加载
     */
    onLoad: function(options) {
        this.api('search/hot/detail', res => {
            console.log(res.data);
            this.setData({
                hotkeys: res.data
            })
        })
    },

    /**
     * 生命周期函数--监听页面初次渲染完成
     */
    onReady: function() {

    },

    /**
     * 生命周期函数--监听页面显示
     */
    onShow: function() {

    },

    /**
     * 生命周期函数--监听页面隐藏
     */
    onHide: function() {

    },

    /**
     * 生命周期函数--监听页面卸载
     */
    onUnload: function() {

    },

    /**
     * 页面相关事件处理函数--监听用户下拉动作
     */
    onPullDownRefresh: function() {

    },

    /**
     * 页面上拉触底事件的处理函数
     */
    onReachBottom: function() {

    },

    /**
     * 用户点击右上角分享
     */
    onShareAppMessage: function() {

    }