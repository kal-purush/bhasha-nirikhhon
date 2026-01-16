// URL do Google Apps Script para receber os uploads (insira a sua URL do Web App do Apps Script)
const scriptURL = "https://script.google.com/macros/s/SEU_SCRIPT_ID/exec"; // TODO: substituir pela URL do seu script

let currentStream = null;
let currentFacing = 'environment'; // câmera atual (padrão traseira)
let lastImageURL = null; // armazena URL da imagem pré-visualizada (para revogar depois)

$(document).ready(function() {
  // Clique no botão "Fazer Upload" -> aciona o input de arquivo
  $('#uploadBtn').on('click', function() {
    $('#fileInput').click();
  });

  // Quando um arquivo é selecionado no input
  $('#fileInput').on('change', function(event) {
    const file = event.target.files[0];
    if (!file) return;
    // Envia o arquivo selecionado (foto ou vídeo)
    sendFile(file);
    // Reseta o input (permite selecionar o mesmo arquivo novamente no futuro)
    $('#fileInput').val('');
  });

  // Clique no botão "Tirar Foto" -> abre a câmera do dispositivo
  $('#cameraBtn').on('click', function() {
    openCamera();
  });

  // Botão "Alternar Câmera" (troca entre frontal e traseira)
  $('#switchCameraBtn').on('click', function() {
    if (!currentStream) return;
    // Para o stream atual antes de alternar
    currentStream.getTracks().forEach(t => t.stop());
    // Alterna o modo da câmera (environment <-> user)
    currentFacing = (currentFacing === 'environment') ? 'user' : 'environment';
    // Solicita o novo stream de vídeo da câmera escolhida
    navigator.mediaDevices.getUserMedia({ video: { facingMode: currentFacing } })
      .then(stream => {
        currentStream = stream;
        document.getElementById('video').srcObject = stream;
        setupZoomSlider(stream);
      })
      .catch(err => {
        console.error('Erro ao alternar câmera:', err);
        toastr.error('Não foi possível alternar a câmera');
      });
  });

  // Botão "📸 Capturar Foto"
  $('#takePhotoBtn').on('click', function() {
    if (!currentStream) return;
    const video = document.getElementById('video');
    // Captura o frame atual do vídeo em um canvas
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    const context = canvas.getContext('2d');
    context.drawImage(video, 0, 0, canvas.width, canvas.height);
    // Converte o frame capturado em Blob (imagem JPEG)
    canvas.toBlob(function(blob) {
      if (blob) {
        // Cria um objeto File a partir do blob para enviar (nomeia como foto_casamento.jpg)
        const imageFile = new File([blob], 'foto_casamento.jpg', { type: blob.type });
        sendFile(imageFile);
      }
    }, 'image/jpeg', 0.9);
    // Fecha o modal da câmera e encerra o stream
    closeCamera();
  });

  // Botão "Fechar" no modal da câmera
  $('#closeCameraBtn').on('click', function() {
    closeCamera();
  });

  // Botão "×" no modal de pré-visualização da foto
  $('#closePreviewBtn').on('click', function() {
    closePreview();
  });
});

// Função para abrir a câmera do dispositivo
function openCamera() {
  // Verifica suporte à API de mídia
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    toastr.error('Navegador não suporta acesso à câmera');
    return;
  }
  // Tenta acessar a câmera traseira primeiro
  navigator.mediaDevices.getUserMedia({ video: { facingMode: { exact: 'environment' } } })
    .then(stream => {
      startCameraStream(stream, 'environment');
    })
    .catch(err => {
      // Se falhar (ex: dispositivo sem câmera traseira), tenta câmera frontal
      console.warn('Câmera traseira indisponível, tentando frontal...', err);
      navigator.mediaDevices.getUserMedia({ video: { facingMode: 'user' } })
        .then(stream => {
          startCameraStream(stream, 'user');
        })
        .catch(err2 => {
          console.error('Não foi possível acessar a câmera:', err2);
          toastr.error('Permissão da câmera negada ou indisponível');
        });
    });
}

// Inicia o stream de vídeo no elemento <video> e exibe o modal da câmera
function startCameraStream(stream, facingMode) {
  currentStream = stream;
  currentFacing = facingMode;
  document.getElementById('video').srcObject = stream;
  // Verifica quantas câmeras há; se houver mais de uma, exibe botão de alternar
  navigator.mediaDevices.enumerateDevices().then(devices => {
    const videoDevices = devices.filter(device => device.kind === 'videoinput');
    if (videoDevices.length > 1) {
      $('#switchCameraBtn').show();
    } else {
      $('#switchCameraBtn').hide();
    }
  });
  // Configura o slider de zoom de acordo com as capacidades da câmera
  setupZoomSlider(stream);
  // Exibe o modal da câmera
  $('#cameraModal').addClass('active');
}

// Configura o slider de zoom baseado nas capacidades do track de vídeo
function setupZoomSlider(stream) {
  const track = stream.getVideoTracks()[0];
  const capabilities = track.getCapabilities();
  const settings = track.getSettings();
  const $zoom = $('#zoomSlider');
  if (capabilities.zoom) {
    // Mapeia limites e passo do zoom para o input range
    $zoom.attr({
      min: capabilities.zoom.min,
      max: capabilities.zoom.max,
      step: capabilities.zoom.step
    });
    if (settings.zoom) {
      $zoom.val(settings.zoom);
    }
    // Aplica o zoom dinamicamente conforme o slider for movido
    $zoom.off('input').on('input', function() {
      track.applyConstraints({ advanced: [{ zoom: Number(this.value) }] });
    });
    $zoom.show();
  } else {
    $zoom.hide();
  }
}

// Envia um arquivo (foto ou vídeo) para o Apps Script (Google Drive/Planilha)
function sendFile(file) {
  const formData = new FormData();
  formData.append('file', file, file.name);
  $.ajax({
    url: scriptURL,
    method: 'POST',
    data: formData,
    processData: false,
    contentType: false,
    success: function(response) {
      console.log('Upload realizado com sucesso:', response);
      // Notificação de sucesso (mensagem varia conforme tipo do arquivo)
      toastr.success(file.type.startsWith('image/') ? 'Foto enviada com sucesso!' : 'Vídeo enviado com sucesso!');
      if (file.type.startsWith('image/')) {
        // Se o upload for de uma foto, exibe o modal de pré-visualização
        showPreview(file);
      }
      // (Para vídeos, apenas a notificação é exibida)
    },
    error: function(error) {
      console.error('Erro no upload:', error);
      toastr.error('Erro ao enviar arquivo. Por favor, tente novamente.');
    }
  });
}

// Exibe o modal de pré-visualização com a imagem enviada
function showPreview(file) {
  // Cria uma URL temporária para exibir a imagem no <img>
  if (lastImageURL) {
    URL.revokeObjectURL(lastImageURL);
  }
  lastImageURL = URL.createObjectURL(file);
  $('#previewImage').attr('src', lastImageURL);
  // Atualiza o link de download com o blob da imagem e nome do arquivo
  $('#downloadPhotoBtn').attr('href', lastImageURL);
  $('#downloadPhotoBtn').attr('download', file.name || 'foto_capturada.jpg');
  // Exibe o modal de pré-visualização
  $('#previewModal').addClass('active');
}

// Fecha/oculta o modal de pré-visualização da foto
function closePreview() {
  $('#previewModal').removeClass('active');
  // Revoga a URL do objeto para liberar memória
  if (lastImageURL) {
    URL.revokeObjectURL(lastImageURL);
    lastImageURL = null;
  }
}

// Fecha/oculta o modal da câmera e para o stream de vídeo
function closeCamera() {
  if (currentStream) {
    currentStream.getTracks().forEach(t => t.stop());
    currentStream = null;
  }
  $('#cameraModal').removeClass('active');
  // Limpa o elemento de vídeo (remove referência ao stream)
  document.getElementById('video').srcObject = null;
}