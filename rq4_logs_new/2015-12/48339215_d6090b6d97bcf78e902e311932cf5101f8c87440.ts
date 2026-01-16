import * as React from "react";

export class MediaQuery {
	private component : React.Component<any, any>;
	private timeoutHandle : number;
	private alwaysUpdateOnResize: boolean;
	
	private currentBreakpoint = '';
	
	private breakpoints = {
		'xs': 600, 
		'sm': 960, 
		'md': 1280, 
		'lg': 1920
	}
	private breakpointNames = ['xs', 'sm', 'md', 'lg']; 
	
	
	private handleResize() {
		var breakpoint = 'gt-lg';
		for(var key in this.breakpoints) {
			var brWidth: number = this.breakpoints[key];
			if(innerWidth < brWidth) {
				breakpoint = key; 
				break;
			}
		}
		
		if(breakpoint != this.currentBreakpoint || this.alwaysUpdateOnResize) {
			this.currentBreakpoint = breakpoint;
			setTimeout(()=>this.component.forceUpdate());
		}
	}
	
	private resizeListener = ()=> {
		clearTimeout(this.timeoutHandle); 
		this.timeoutHandle = setTimeout(this.handleResize.bind(this), 100);
	}	
	
	
	compute(table: Object): any {
		var breakpointsToCheck = [this.currentBreakpoint];
		var breakpointIndex = this.breakpointNames.indexOf(this.currentBreakpoint); 
		breakpointIndex = breakpointIndex > -1 ? breakpointIndex - 1 : 2; 
		
		for(var i = breakpointIndex; i >= 0; i --) {
			breakpointsToCheck.push(`gt-${this.breakpointNames[i]}`); 
		}
		
		breakpointsToCheck.push('default'); 
		breakpointsToCheck.push('$'); 

		for(var breakpoint of breakpointsToCheck) {
			if(typeof(table[breakpoint]) != 'undefined') return table[breakpoint];
		}
		return null;		
	}
	
	release() {
		clearTimeout(this.timeoutHandle);
		removeEventListener('resize', this.resizeListener); 
	}

	constructor(component : React.Component<any, any>, alwaysUpdateOnResize: boolean = false) {
		this.component = component;
		this.alwaysUpdateOnResize = alwaysUpdateOnResize
		
		addEventListener('resize', this.resizeListener);
		
		this.handleResize();
		
	}
}