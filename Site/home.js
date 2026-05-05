(function () {
  function isAcceptedImageFile(file) {
    if (!file) return false;
    const name = (file.name || "").toLowerCase();
    const byName =
      name.endsWith(".jpg") ||
      name.endsWith(".jpeg") ||
      name.endsWith(".heic") ||
      name.endsWith(".heif");
    const mime = (file.type || "").toLowerCase();
    const byMime =
      mime === "image/jpeg" ||
      mime === "image/jpg" ||
      mime === "image/heic" ||
      mime === "image/heif";
    return byName || byMime;
  }

  document.addEventListener("DOMContentLoaded", function () {
    const uploadTrigger = document.getElementById("uploadTrigger");
    const imageInput = document.getElementById("imageInput");
    const uploadStatus = document.getElementById("uploadStatus");

    if (!uploadTrigger || !imageInput || !uploadStatus) {
      return;
    }

    function setUploadIdleState() {
      uploadTrigger.textContent = "ENVIE UMA FOTO";
      uploadTrigger.disabled = false;
    }

    setUploadIdleState();

    uploadTrigger.addEventListener("click", function () {
      imageInput.click();
    });

    imageInput.addEventListener("change", async function (event) {
      const file = event.target.files && event.target.files[0];
      if (!file) return;

      if (!isAcceptedImageFile(file)) {
        uploadStatus.textContent = "Envie apenas JPG, JPEG, HEIC ou HEIF.";
        imageInput.value = "";
        setUploadIdleState();
        return;
      }

      uploadStatus.textContent = "Enviando imagem...";
      uploadTrigger.textContent = "Enviando...";
      uploadTrigger.disabled = true;

      const formData = new FormData();
      formData.append("imagem", file);

      try {
        const response = await fetch("http://127.0.0.1:5000/gerar", {
          method: "POST",
          body: formData
        });

        const result = await response.json();
        if (response.ok && result.ok && result.nome_original) {
          window.location.href = "transicao.html?job_id=" + encodeURIComponent(result.nome_original);
          return;
        }

        uploadStatus.textContent = result.erro || "Erro ao enviar imagem.";
        setUploadIdleState();
      } catch (error) {
        uploadStatus.textContent = "Erro de conexão com o servidor.";
        setUploadIdleState();
      }
    });
  });
})();
