const d = document;

export default function SearchFilters(input, selector){
   d.addEventListener("Keyup",(e)=>{
   if(e.target.matches(input)) {
   //console.log(e.key);
   d.querySelectorAll(selector).forEach(el =>
    el.textContent.toLowerCase().includes(e.target.value)
    ?el.classList.remove("filter")
    :el.classList.add("filter")
   );

   } 
  });

} 