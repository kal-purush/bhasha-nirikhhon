(function(µ,SMOD,GMOD,HMOD,SC){

	let request=GMOD("request");
	SC=SC({
		Node:"NodePatch",
		query:"queryParam"
	});

	let createMenu=function(json)
	{
		let menu=document.createElement("DIV");
		menu.id="menu";
		SC.Node.traverse(json,function(entry,parent,parentResult,info)
		{
			let {container,path}=parentResult;
			if(info.depth!==0)
			{
				let details=document.createElement("DETAILS");
				let summary=document.createElement("SUMMARY");
				//summary.textContent=entry.name;
				let span=document.createElement("SPAN");
				let sub
				span.textContent=entry.name;
				summary.appendChild(span);
				details.appendChild(summary);

				if(entry.hasCase)
				{
					let anchor=document.createElement("A");
					anchor.textContent=entry.name;
					anchor.target="frame";
					anchor.href="presenter.html?showcase="+path+entry.name;
					anchor.classList.add("summaryLink");
					container.appendChild(anchor);
				}

				container.appendChild(details);
				let nextContainer=document.createElement("DIV");
				nextContainer.classList.add("menu-container");
				details.appendChild(nextContainer);

				container=nextContainer;
				path+=entry.name+"/";
			}
			for(let sc of entry.cases)
			{
				let anchor=document.createElement("A");
				anchor.textContent=sc;
				anchor.target="frame";
				anchor.href="presenter.html?showcase="+path+sc;
				container.appendChild(anchor);
			}
			return {container,path};
		},{initial:{container:menu,path:""}});
		return menu;
	}

	document.body.classList.add("blocked");
	request.json("showcases.json")
	.then(showcases=>
	{

		document.body.appendChild(createMenu(showcases));
		document.body.classList.remove("blocked");

		if(SC.query.showcase)
		{
			let caseUrl="presenter.html?showcase="+SC.query.showcase;
			if("index" in SC.query) caseUrl+="&index="+SC.query.index;
			frame.location=caseUrl;
		}
	});

})(Morgas,Morgas.setModule,Morgas.getModule,Morgas.hasModule,Morgas.shortcut);