
    // ----------------------------------------------------------------------------------------------------------------
    const mylisttitle =
      (props, { $h }) => {
        //let foo = 'bar';
        return () => $h`
        <h1 style="background-color:green;"><b>Custom Component:</b></h1>
        `;
      }

    // ----------------------------------------------------------------------------------------------------------------
    const mylistitem =
      (props, { $h }) => {
        let foo = 'bar';

        return () => $h`
        <span>
        <div class="navbar">
          <div class="navbar-bg"></div>
          <div class="navbar-inner">
            <div class="left">
              <a href="#" class="back link">
                <i class="icon icon-back"></i>
                <span>Back</span>
              </a>
            </div>
          </div>
        </div>
    
        <my-list-title />
        <a class="link external" href="${props.http}">Open LINK</a>
        <ul>
          <li class="item-content" id="${props.id}" foo="${props.foo}">${props.id}</li>
          <li class="item-content" id="${props.id}" foo="${props.foo}">${foo}</li>
          <li class="item-content" id="${props.id}" foo="${props.foo}">000</li>
          <li class="item-content" id="${props.id}" foo="${props.foo}">000</li>
        </ul>
    
        </span>
        `;
      }

    // --------------------------------------COMPONENTS REGSTRATION-------------------------------------------------------
    Framework7.registerComponent(
      // component name, // component function
      'my-list-title', mylisttitle
    )

    Framework7.registerComponent(
      // component name, // component function
      'my-list-item', mylistitem
    )