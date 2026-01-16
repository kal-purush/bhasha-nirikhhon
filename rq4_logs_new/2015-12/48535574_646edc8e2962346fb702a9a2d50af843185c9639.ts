//Add your initialization logic here
module famousPainters.App {
  import Painters = famousPainters.Painters
  import Render = famousPainters.Render

  const urlDATA = 'JSON/famousPainters.json'
  export var painterCollection= new Painters.Collection<Painters.Painter>()
  export var styleCollection = new Painters.Collection<string>()
  var renderer: Render.Renderer
  var loader: DataLoader



  export class DataLoader {
    constructor(public url: string) {}

    loadJSON(){
      $.getJSON(this.url)
        .done((data) => this.MapData(data))
        .fail((data,textStatus,error)=>{
          renderer.renderError(error)
        })
    }

    MapData(data: any) {
      if (data) {

        // data comes from JSON file
        let painters: any[] = data.famousPainters;
        painters.forEach((painter) => painterCollection.items.push(new Painters.Painter(
            <Painters.IPainter>{
              name: painter.name,
              style: painter.style
            },
            <Painters.IPainterInformation>{
                birthDate: painter.information.birthDate,
                birthPlace: painter.information.birthPlace,
                nationality: painter.information.nationality
              },
            <Painters.IPainterExample[]>painter.examples
            )))
        renderer.renderPainterList(painterCollection)
      } else {
        renderer.renderError ('no data')
      }
    }

    // LoadPainter when selector changes
    LoadPainter() {
      var selector = <HTMLSelectElement> document.getElementById('PainterSelector')
      try {
        var painter = painterCollection.items
          .filter(item=>item.name === selector.value)
          .reduce((item)=><Painters.Painter>item)
          renderer.renderPainter(painter)
      } catch (ex) {
        renderer.renderError(ex.message)
      }
    }
  }

  /**
   * LoadData: Initialize DataLoader and request JSON file.
   */
  function LoadData(): void {
    loader = new DataLoader(urlDATA);
    loader.loadJSON();
  }

  window.onload = () => {
    renderer = new Render.Renderer()
    LoadData()
    var painterSelector = <HTMLSelectElement> document.getElementById('PainterSelector')
    painterSelector.onchange = ()=>loader.LoadPainter()


  }
}