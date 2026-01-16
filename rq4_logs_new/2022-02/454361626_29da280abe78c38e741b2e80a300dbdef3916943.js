window.onload = function() {

  // Constants.
  const sessionId = '' + Math.floor(Math.random() * Math.pow(2,40)); // Don't care about predictability.
  const wsUrl = 'ws://localhost:4243';
  const wsMaxRetries = 3;
  const wsReplyTimeout = 300000;
  
  const agentColours = {
    infected: 'red',
    immune: 'grey',
    identified: 'blue',
    identifiable: 'yellow',
    notIdentifiable: 'green'
  };
  const agentDotRadius = 5; // Canvas pixels.

  const fps = 20; // Maximal canvas refreshes per second.

  // Queue of [call, callback]. Call wakeUpRPCLoop to get new calls processed.
  let rpcCallFifo = [];
  let wakeUpRPCLoop = () => {}
  let rpcLoopRunning = false;

  async function rpcLoop()
  {
    rpcLoopRunning = true;
    for(let attempt = 0; attempt < wsMaxRetries; ++attempt)
    {
      let sock = null;
      try
      {
        sock = new WebSocket(wsUrl);

        // Wait for open.
        await new Promise((resolve, reject) => {
          sock.onopen = resolve;
          sock.onerror = reject;
        });
        
        // Process pending calls one by one.
        while(true)
        {
          if(rpcCallFifo.length > 0)
          {
            let [call, callback] = rpcCallFifo.shift();
            let replyReceiver = new Promise((resolve, reject) => {
              sock.onmessage = ev => resolve(ev.data);
              sock.onerror = err => {
                rpcCallFifo.unshift([call, callback]);
                reject(err);
              };
              setTimeout(() => reject('WebSocket receive timeout.'), wsReplyTimeout);
            });
            sock.send(call);
            callback(await replyReceiver);
          }
          else
          {
            // Sleep until next call is enqueued.
            await new Promise(resolve => {
              wakeUpRPCLoop = resolve;
            });
          }
        }
      }
      catch(ex)
      {
        console.error(ex + '\n' + ex.message);
        if(sock !== null)
        {
          sock.close();
        }
      }
    }

    alert('Websocket connection error.');
    rpcLoopRunning = false;
  }


  // InvokeRPC calls should not be interleaved.
  async function invokeRPC(call)
  {
    if(!rpcLoopRunning)
    {
      rpcLoop();
    }

    call['session-id'] = sessionId;
    let errorMsg = '';
    try
    {
      let replyMsg = await new Promise(resolve => {
        rpcCallFifo.push([JSON.stringify(call), resolve]);
        wakeUpRPCLoop();
      });
      let reply = JSON.parse(replyMsg);
      if(reply.status === 'ok')
      {
        return reply;
      }
      else if(reply.status === 'error')
      {
        errorMsg = 'Server error: ' + reply.message;
        return null;
      }
      else
      {
        errorMsg = "Can't parse reply: " + replyMsg;
        return null;
      }
    }
    catch(ex)
    {
      errorMsg = ex.message;
      return null;
    }
    finally
    {
      document.getElementById('error-message').textContent = errorMsg;
    }
  }

  async function loadSimulation(agentCount, seed, modelNames, modelParams)
  {
    let startCall = {
      call: 'start',
      agents: agentCount,
      seed: seed,
      models: modelNames,
      params: modelParams
    };
    let reply = await invokeRPC(startCall);
    if(reply !== null)
    {
      drawReport(reply.report);
      return true;
    }
    else
    {
      return false;
    }
  }

  function parseMobilityParams(modelNames, data)
  {
    let pNames = {
      bsa: ['maxVelocity', 'maxAcceleration', 'maxTurn'],
      smooth: ['waypointCount', 'clusterCount', 'agentSpeed'],
      levy: ['flightLengthCoefficient', 'pauseTimeCoefficient', 'maxPause']
    }

    let params = {bounds: [data['area-width'], data['area-height']]};
    for (let pname of pNames[modelNames.mobilityModel])
    {
      params[pname] = data[modelNames.mobilityModel + '-' + pname];
    }
    return params;
  }

  function parseDiscoveryParams(data)
  {
    if(data['identificationModel'].startsWith('gaussian'))
    {
      alert('Not implemented');
    }
    else if(data['identificationModel'].startsWith('trigger'))
    {
      let profiles = [{
        prevalence: data['id-always'],
        frequencyDist: {mean: 1, stdDev: 0},
        discoveryTimeLimit: null
      }];

      let regularParams = ['prevalence', 'discoveryTimeLimit'];
      let distParams = ['frequencyDist'];

      for(let i = 0; `pt-group-${i}-${regularParams[0]}` in data; ++i)
      {
        let profile = {};
        for(let param of regularParams)
        {
          profile[param] = data[`pt-group-${i}-${param}`];
        }
        for(let param of distParams)
        {
          let base = `pt-group-${i}-${param}-`;
          profile[param] = {
            mean: data[base + 'mean'],
            stdDev: data[base + 'stdDev']
          }
        }
        profiles.push(profile);
      }

      return {behaviours: profiles};
    }
    else
    {
      throw 'Invalid discovery model.';
    }
  }

  function parseModelParams(modelNames, data)
  {
    let agentCount = data['agents-total'];

    // Mobility.
    let mobParams = parseMobilityParams(modelNames, data);

    // Identification.
    let idParams;
    if(modelNames.identificationModel === 'dummy')
    {
      idParams = null;
    }
    else if(modelNames.identificationModel === 'signal')
    {
      idParams = [];
      for(let i = 0; `sigprof-${i}-senderPrevalence` in data; ++i)
      {
        idParams[i] = {};
        for(let param of ['senderPrevalence', 'receiverPrevalence', 'transmissionRange', 'signalFrequency'])
        {
          idParams[i][param] = data[`sigprof-${i}-${param}`];
        }
      }
    }
    else
    {
      // Discovery.
      idParams = {
        discoveryModel: parseDiscoveryParams(data),      
        transmisisonRadius: data['id-transmissionRadius'],
        discoveryTime: data['id-discoveryTime']
      }

      // Broadcast models (0 to 2).
      let bcModelCount = data['identificationModel'].endsWith('+broadcast') ? 1 :
                         data['identificationModel'].endsWith('+broadcast2') ? 2 : 
                         0;

      let bcParams = [];
      for(let bcModel = 1; bcModel < bcModelCount + 1; ++bcModel)
      {
        bcParams.push({
          broadcasterCount: Math.round(data['bcFraction' + bcModel] * agentCount),
          receiverCount: Math.round(data['bcRecvFraction' + bcModel] * agentCount),
          broadcastRadius: data['bcRadius' + bcModel],
          identificationChance: data['bcIdChance' + bcModel],
          frequencyDistribution: {
            mean: data['bcFreq-mean' + bcModel],
            stdDev: data['bcFreq-dev' + bcModel]
          }
        });
      }

      if(bcModelCount == 1)
      {
        idParams = [idParams, bcParams[0]];
      }
      else if(bcModelCount == 2)
      {
        idParams = [idParams, bcParams];
      }
    }

    // Infection.
    let infParams = {};
    let prefix = modelNames.infectionModel + '-';
    for(let [key, value] of Object.entries(data))
    {
      if(key.startsWith(prefix))
      {
        infParams[key.substr(prefix.length)] = value;
      }
    }

    return {
      mobModel: mobParams,
      idModel: idParams,
      infModel: infParams
    }
  }

  async function startOrRestart()
  {
    // Stop any current run.
    if(document.getElementById('runpause-button').hasAttribute('data-running'))
    {
      runOrPause();
    }

    // Convert form contents to the expected format.
    let form = document.getElementById('param-form');
    let data = {}
    for(let [name, value] of new FormData(form))
    {
      // Parse number inputs as integers or doubles.
      if(form.elements[name].type === 'number')
      {
        let step = form.elements[name].getAttribute('step');
        value = step && step.indexOf('.') >= 0 ? parseFloat(value) : parseInt(value);
      }
      data[name] = value;
    }
    let agentCount = data['agents-total'];
    let seed = data['seed'];
    
    // Extract model names and params.
    let modelNames = {};
    for(let cat of ['mobilityModel', 'identificationModel', 'infectionModel'])
    {
      modelNames[cat] = data[cat];
    }
    let modelParams = parseModelParams(modelNames, data);

    // Load simulation with parameters.
    let success = await loadSimulation(agentCount, seed, modelNames, modelParams);

    // Update control buttons.
    document.getElementById('start-button').textContent = success ? 'Restart' : 'Start';
    document.getElementById('runpause-button').textContent = 'Run';
    document.getElementById('runpause-button').removeAttribute('disabled');
    document.getElementById('step-button').removeAttribute('disabled');
    document.getElementById('multistep-button').removeAttribute('disabled');
  }

  async function doSteps(count)
  {
    let stepCall = {
      call: 'steps',
      amount: count
    };
    let reply = await invokeRPC(stepCall);
    if(reply !== null)
    {
      drawReport(reply.report);
      return true;
    }
    else
    {
      return false;
    }
  }

  function runOrPause()
  {
    let rpButton = document.getElementById('runpause-button');
    let stepButton = document.getElementById('step-button');
    let msStepButton = document.getElementById('multistep-button');
    if(rpButton.hasAttribute('data-running'))
    {
      rpButton.removeAttribute('data-running');
      rpButton.textContent = 'Run';
      stepButton.removeAttribute('disabled');
      msStepButton.removeAttribute('disabled');
    }
    else
    {
      rpButton.textContent = 'Pause';
      stepButton.setAttribute('disabled', '');
      msStepButton.setAttribute('disabled', '');
      rpButton.setAttribute('data-running', '');
      let speedInput = document.getElementById('simulation-speed');
      let stepCounter = document.getElementById('step-count');
      (async function stepper(startTime, startSpeed, startStep)
      {
        if(rpButton.hasAttribute('data-running'))
        {
          let currentStep = parseInt(stepCounter.textContent);
          let speed = parseInt(speedInput.value);

          if(speed !== startSpeed)
          {
            // Recompute timings when speed changes.
            startTime = Date.now();
            startSpeed = speed;
            startStep = currentStep;            
          }

          let goalStep = Math.max(currentStep + 1, Math.min(currentStep + 2 * speed, Math.floor((Date.now() - startTime) / 1000 * speed)));
          if(await doSteps(goalStep - currentStep))
          {
            setTimeout(() => stepper(startTime, startSpeed, startStep), 1000 / fps);
          }
        }
      })(Date.now(), parseInt(speedInput.value), parseInt(stepCounter.textContent));
    }
  }

  function drawReport(report)
  {
    // Update step counter.
    document.getElementById('step-count').textContent = report.step;

    // Draw simulation area.
    let formData = document.getElementById('param-form').elements;
    let areaWidth = formData['area-width'].value;
    let areaHeight = formData['area-height'].value;

    let canvas = document.getElementById('simulation-display');
    let context = canvas.getContext('2d');
    context.clearRect(0, 0, canvas.width, canvas.height);
    let stateCounts = {
      infected: 0,
      immune: 0,
      identified: 0,
      identifiable: 0,
      notIdentifiable: 0
    }
    let correctX = pos => pos.x / areaWidth  * canvas.width;
    let correctY = pos => pos.y / areaHeight * canvas.height;

    // First draw attack lines.
    for(let agent of report.agents)
    {
      for(let target of agent.currentlyTargeting)
      {
        context.beginPath();
        context.setLineDash([2, 2]);
        context.moveTo(correctX(agent.position), correctY(agent.position));
        context.lineTo(correctX(target), correctY(target));
        context.stroke();
        context.setLineDash([]);
      }
    }

    // Now draw coloured circles for each agent.
    for(let agent of report.agents)
    {
      let agentState = 
        agent.infected     ? 'infected'     :
        agent.immune       ? 'immune'       :
        agent.identified   ? 'identified'   :
        agent.identifiable ? 'identifiable' :
                             'notIdentifiable';
      
      ++stateCounts[agentState];

      context.beginPath();
      context.arc(correctX(agent.position), correctY(agent.position), agentDotRadius, 0, 2 * Math.PI, false);
      context.fillStyle = agentColours[agentState];
      context.fill();
      context.stroke();
    }

    // Update counts.
    for(let [name, count] of Object.entries(stateCounts))
    {
      let el = document.getElementById('legend-' + name).getElementsByClassName('legend-count')[0];
      el.textContent = '(' + count + ')';
    }
  }

  // Colour legend.
  for(let [name, colour] of Object.entries(agentColours))
  {
    document.getElementById('legend-' + name).getElementsByClassName('legend-dot')[0].style.color = colour;
  }

  // Form interactions.
  let form = document.getElementById('param-form');

  // Simulation seed refreshing.
  let seedInput = form.elements['seed'];
  let refreshSeed = () => {
    let newSeed = Math.floor(Math.random() * seedInput.max);
    seedInput.value = newSeed;
  };
  refreshSeed();
  form.elements['refresh-seed-button'].addEventListener('click', refreshSeed);

  // Simulation control buttons.
  document.getElementById('start-button').addEventListener('click', startOrRestart);
  document.getElementById('runpause-button').addEventListener('click', runOrPause);
  document.getElementById('step-button').addEventListener('click', () => doSteps(1));
  document.getElementById('multistep-button').addEventListener('click', () => {
    let currentSpeed = Number.parseInt(document.getElementById('simulation-speed').value);
    doSteps(currentSpeed);
  });


  // Canvas resize buttons.
  let display = document.getElementById('simulation-display');
  document.getElementById('upsize-button').addEventListener('click', () => {
    display.width *= 1.25;
    display.height *= 1.25;
  });
  document.getElementById('downsize-button').addEventListener('click', () => {
    display.width /= 1.25;
    display.height /= 1.25;
  });

  // Submodel switchers.
  for(let switcher of document.getElementsByClassName('model-switcher'))
  {
    switcher.addEventListener('change', ev => {
      for(let option of ev.target.options)
      {
        for(el of document.getElementsByClassName(option.value + '-params'))
        {
          el.style.display = 'none';
        }
      }
      for(let option of ev.target.selectedOptions)
      {
        for(el of document.getElementsByClassName(option.value + '-params'))
        {
          el.style.display = '';
        }
      }
    });
  }

  // Trigger row template table expander.
  let rowGroupResetters = {}
  for(let rowGroup of document.getElementsByClassName('rowgroup-template'))
  {
    let template = rowGroup.content;

    function addRow()
    {
      let tableBody = document.getElementById(`${rowGroup.id}-tablebody`);
      let rowCount = tableBody.getElementsByTagName('tr').length;
      let newRow = template.cloneNode(true).firstChild;
      for(let newCell of newRow.getElementsByTagName('input'))
      {
        newCell.name = newCell.name.replace("%", rowCount);
      }
      tableBody.appendChild(newRow);
      newRow.getElementsByClassName(`${rowGroup.id}-deleter`)[0].addEventListener('click', () => {
        tableBody.removeChild(newRow);
      });
    }

    document.getElementById(`${rowGroup.id}-adder`).addEventListener('click', addRow);
    addRow();

    rowGroupResetters[rowGroup.id] = count => {
      for(let row of document.querySelectorAll(`#${rowGroup.id}-tablebody > tr`))
      {
        row.remove();
      }
      for(let i = 0; i < count; ++i)
      {
        addRow();
      }
    }
  }

  // Standard Levy parameters.
  let levyInputs = {
    a: form.elements['levy-flightLengthCoefficient'],
    b: form.elements['levy-pauseTimeCoefficient'],
    t: form.elements['levy-maxPause']
  }
  let levyParamSelect = document.getElementById('levy-paramset');
  for(let [param, field] of Object.entries(levyInputs))
  {
    field.addEventListener('change', ev => {
      let currentSelection = levyParamSelect.selectedOptions[0];
      if(ev.target.value != currentSelection.getAttribute(`data-${param}`))
      {
        levyParamSelect.value = '';
      }
    });
    levyParamSelect.addEventListener('change', () => {
      if(levyParamSelect.value !== '')
      {
        field.value = levyParamSelect.selectedOptions[0].getAttribute(`data-${param}`);
      }
    });
  }

  // Save and load buttons.
  let loadButton = document.getElementById('load-button');
  let saveButton = document.getElementById('save-button');
  if(localStorage.getItem('saved-parameters') !== null)
  {
    loadButton.removeAttribute('disabled');
  }
  loadButton.addEventListener('click', () => {
    let saved = JSON.parse(localStorage.getItem('saved-parameters'));
    for(let [group, count] of Object.entries(saved.rowCounts))
    {
      rowGroupResetters[group](count);
    }
    for(let [name, value] of Object.entries(saved.form))
    {
      let elem = form.elements[name];
      if(elem)
      {
        elem.value = value;
        elem.dispatchEvent(new Event('change'));
      }
    }
  });
  saveButton.addEventListener('click', () => {
    let saveform = {};
    for(let [name, value] of new FormData(form))
    {
      saveform[name] = value;
    }
    let savedata = {form: saveform, rowCounts: {}};
    for(let rowGroup of document.getElementsByClassName('rowgroup-template'))
    {
      savedata.rowCounts[rowGroup.id] = document.querySelectorAll(`#${rowGroup.id}-tablebody > tr`).length;
    }
    localStorage.setItem('saved-parameters', JSON.stringify(savedata));
    loadButton.removeAttribute('disabled');
  });
};