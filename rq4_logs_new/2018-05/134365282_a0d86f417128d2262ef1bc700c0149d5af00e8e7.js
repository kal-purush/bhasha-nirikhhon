function BinaryClock( cont )
{
	// Initialize variables, that will be used
	// by public methods, with default values
	var sOrientation = 'btt';
	var bShowSeconds = true;
	var bSecondsInTheMiddle = false;
	var iRankBits = 4;
	
	// Other vars
	var asToggleClasses = [ 'on' , 'off' ]; // Classes for 1 and 0
	var asHoursMinutes = [ 'h' , 'm' ]; // Hours, Minutes
	var sSeconds = 's'; // Seconds - in case they are enabled
	var asDecimalEntities = [ 'd' , 'e' ]; // For bits order that represents each digit
	var iLoop = 0; // To keep setInterval ID
	
	// Settings for container
	var
	 iHorizontalSquaresCount
	,iVerticalSquaresCount
	,sOrientationAxis
	,iXKoef
	,asHMS
	,asDE;
	
	// Clock container and the container to append to
	var oBody = null;
	var oClockContainer = null;
	
	// Constructor :)
	var construct = function( cont )
	{
		oBody = document.createElement( 'div' );
		oBody.className = 'binary-clock-cont';
		appendClock( cont );
		buildClockContainer( );
	};
	
	var appendClock = function( cont )
	{
		if( typeof cont != 'undefined' )
		{
			if( document.querySelector( cont ) ? document.querySelector( cont ).nodeType == 1 : false )
				document.querySelector( cont ).appendChild( oBody );
			else if( document.getElementById( cont ) ? document.getElementById( cont ).nodeType == 1 : false )
				document.getElementById( cont ).appendChild( oBody );
		}
		
		return this;
	};
	
	var setTitle = function( arr )
	{
		oBody.setAttribute( 'title' , arr.join( ':' ) );
	};
	
	// Setup variables for the container, before build it
	var setupBuildClockContainerParameters = function( )
	{
		sOrientationAxis = sOrientation.match( /btt|ttb/ ) ? 'x' : 'y';
		iXKoef = sOrientation.match( /btt|rtl/ ) ? ( ( iRankBits - 1 ) * -1 ) : 0;
		asHMS = asHoursMinutes.slice( 0 ); // Copy for local use
		asDE = asDecimalEntities.slice( 0 ); // ... same here
		
		if( bShowSeconds )
			asHMS.splice( bSecondsInTheMiddle ? 1 : 2 , 0 , sSeconds );
		
		if( iRankBits != 4 )
			asDE.splice( 1 , 1 );
		
		if( sOrientationAxis == 'x' )
		{
			iHorizontalSquaresCount = asHMS.length * asDE.length;
			iVerticalSquaresCount = iRankBits;
		}
		else // if sOrientationAxis == 'y'
		{
			iVerticalSquaresCount = asHMS.length * asDE.length;
			iHorizontalSquaresCount = iRankBits;
		}
	};
	
	// Setup the HTML for the clock container
	var buildClockContainer = function( )
	{
		setupBuildClockContainerParameters( );
		
		oBody.innerHTML = '';
		
		for( var i = 0; iVerticalSquaresCount * iHorizontalSquaresCount > i; ++i )
		{
			var oTempElement = document.createElement( 'span' );
			oBody.appendChild( oTempElement );
			
			var sHandleTimeType = asHMS[ sOrientationAxis == 'x' ? parseInt( i / asDE.length ) % asHMS.length : parseInt( i / ( iRankBits * asDE.length ) ) ];
			var sHandleRankType = asDE[ parseInt( i / ( sOrientationAxis == 'x' ? 1 : iHorizontalSquaresCount ) ) % asDE.length ];
			
			var sIDLetters = sHandleTimeType + sHandleRankType;
			var sIDDigit = Math.pow( 2 , Math.abs( ( sOrientationAxis == 'x' ? parseInt( i / iHorizontalSquaresCount ) : i % iHorizontalSquaresCount ) + iXKoef ) )
			
			oTempElement.setAttribute( 'id' , sIDLetters + sIDDigit.toString( 10 ) );
			oTempElement.className = sIDLetters + ' ' + asToggleClasses[ 0 ] + ( i % iHorizontalSquaresCount ? '' : ' lf' ) + ' item-' + sHandleTimeType;
		}
	};
	
	// Redraw the clock
	var rebuildClock = function( )
	{
		stopClock( );
		buildClockContainer( );
		runClock( );
	};
	
	// Refresh clock "arrows"
	var refreshClockTime = function( )
	{
		var oDate = new Date( );
		var iHours = '0' + oDate.getHours( ).toString( );
		var iMinutes = '0' + oDate.getMinutes( ).toString( );
		var iSeconds = '0' + oDate.getSeconds( ).toString( );
		
		if( !parseInt( iSeconds ) ) // Update them per minute
		{
			setClockDigits( 'h' , iHours );
			setClockDigits( 'm' , iMinutes );
		}
		
		setTitle( [ iHours.substr( -2 ), iMinutes.substr( -2 ), iSeconds.substr( -2 ) ] );
		
		if( bShowSeconds )
			setClockDigits( 's' , '0' + iSeconds.toString( ) );
	};
	
	// Draw each "arrow"
	var setClockDigits = function( l , d )
	{
		try
		{
			d = iRankBits == 4 ? d.substr( -2 ).split( '' ) : [ d ];
			for( var i = 0; asDE.length > i; ++i )
				for( var j = 0; iRankBits > j; ++j )
				{
					var oN = document.getElementById( l + String.fromCharCode( 'd'.charCodeAt( 0 ) + i ) + Math.pow( 2 , j ) );
					var bN = ( d[ i ] & Math.pow( 2 , j ) ) ? true : false;
					oN.className = oN.className.replace( asToggleClasses[ bN * 1 ] , asToggleClasses[ !bN * 1 ] );
				}
		}
		catch( e )
		{
			if( e.name == 'TypeError' )
				console.log( 'Whoops! I would like to share with you that, with a big probability, you didn\'t provide me with a valid container to appear in! Try again, huh? [' + e.message + ']' );
		}
	};
	
	// Run the "mechanism"
	var runClock = function( )
	{
		// Setup
		setTimeout( function( ) {
		var oDate = new Date( );
		setClockDigits( 'h' , '0' + oDate.getHours( ).toString( ) );
		setClockDigits( 'm' , '0' + oDate.getMinutes( ).toString( ) );
		if ( bShowSeconds ) setClockDigits( 's' , '0' + oDate.getSeconds( ).toString( ) );
		} , 0 );
		
		// Loop
		iLoop = setInterval( refreshClockTime , 1000 );
		
		return returnObject;
	};
	
	// Stop clock
	var stopClock = function( )
	{
		clearInterval( iLoop );
		
		return returnObject;
	};
	
	// "Public" members
	var returnObject =
	{
		body: oBody,
		run: runClock,
		stop: stopClock,
		appendTo: appendClock,
		showSeconds: function( )
		{
			bShowSeconds = true;
			rebuildClock( );
			return this;
		},
		hideSeconds: function( )
		{
			bShowSeconds = false;
			rebuildClock( );
			return this;
		},
		setOrientation: function( orient )
		{
			if( [ 'ttb' , 'btt' , 'ltr' , 'rtl' , 'vertical' , 'horizontal' ].indexOf( orient ) != -1 )
			{
				if ( orient == 'vertical' ) sOrientation = 'btt';
				else if ( orient == 'horizontal' ) sOrientation = 'rtl';
				else sOrientation = orient;
				rebuildClock( );
			}
			return this;
		},
		setSecondsPosition: function( pos )
		{
			if( pos.match( /center|middle/ ) )
				bSecondsInTheMiddle = true;
			else if( pos.match( /end|side/ ) )
				bSecondsInTheMiddle = false;
			else
				return this;
			rebuildClock( );
			return this;
		},
		setType: function( typ )
		{
			if( typ.match( /wide|long|big/ ) )
				iRankBits = 7;
			else if( typ.match( /tight|short|small/ ) )
				iRankBits = 4;
			else
				return this;
			rebuildClock( );
			return this;
		},
		setTheme: function( thm )
		{
			oBody.className = 'binary-clock-cont';
			oBody.classList.add( thm );
			return this;
		}
	};
	
	// "Emulate" constructor
	construct.call( this , cont );
	
	return returnObject;
}