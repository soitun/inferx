(function () {
    function setElementText(target, text) {
        if (!target) {
            return;
        }
        if (typeof target.value === 'string') {
            target.value = text;
            return;
        }
        target.textContent = text;
    }

    function appendElementText(target, text) {
        if (!target) {
            return;
        }
        if (typeof target.value === 'string') {
            target.value += text;
            return;
        }
        target.textContent += text;
    }

    function setBusyState(button, processing, runLabel, isBusy) {
        if (button) {
            button.disabled = isBusy;
        }
        if (processing) {
            processing.style.display = isBusy ? '' : 'none';
            if (isBusy) {
                processing.style.width = '15px';
                processing.style.height = '15px';
            }
        }
        if (runLabel) {
            runLabel.style.display = isBusy ? 'none' : '';
        }
    }

    function normalizePathBase(value) {
        return String(value || '').replace(/\/+$/, '');
    }

    function buildUrlFromSegments(basePath, segments) {
        const prefix = normalizePathBase(basePath);
        const encodedPath = segments
            .flatMap((segment) => String(segment || '').split('/'))
            .filter((segment) => segment !== '')
            .map((segment) => encodeURIComponent(segment))
            .join('/');
        return new URL(prefix + '/' + encodedPath, window.location.origin).toString();
    }

    function cloneMapValue(value) {
        if (value == null || typeof value !== 'object') {
            return value;
        }
        return JSON.parse(JSON.stringify(value));
    }

    function formatLatencyValue(value) {
        const text = String(value || '').trim();
        return text === '' ? '-' : text;
    }

    function createInferenceController(config) {
        const context = config || {};
        let abortController = null;
        let inferenceInFlight = false;
        let firstOutputNotified = false;

        function getElement(id) {
            return document.getElementById(id);
        }

        function notifyStart() {
            inferenceInFlight = true;
            firstOutputNotified = false;
            if (typeof context.onRequestStart === 'function') {
                context.onRequestStart();
            }
        }

        function notifyFinish() {
            inferenceInFlight = false;
            if (typeof context.onRequestFinish === 'function') {
                context.onRequestFinish();
            }
        }

        function notifyFirstOutput() {
            if (firstOutputNotified) {
                return;
            }
            firstOutputNotified = true;
            if (typeof context.onFirstOutput === 'function') {
                context.onFirstOutput();
            }
        }

        function loadImage() {
            const urlInput = getElement('urlInput');
            const preview = getElement('preview');
            const url = String(urlInput && urlInput.value || '').trim();

            if (!preview) {
                return;
            }

            if (url.endsWith('.jpg') || url.endsWith('.jpeg')) {
                preview.src = url;
                preview.style.display = 'block';
                return;
            }

            window.alert('Please enter a valid JPEG image URL ending with .jpg or .jpeg');
            preview.style.display = 'none';
        }

        function loadAudio() {
            const urlInput = getElement('urlInput');
            const audioPreview = getElement('audioPreview');
            const url = String(urlInput && urlInput.value || '').trim();

            if (!audioPreview) {
                return;
            }

            if (url.endsWith('.wav') || url.endsWith('.mp4') || url.endsWith('.mp3')) {
                audioPreview.src = url;
                audioPreview.style.display = 'block';
                audioPreview.load();
                return;
            }

            window.alert('Please enter a valid audio URL ending with .wav or .mp4 or .mp3');
            audioPreview.style.display = 'none';
        }

        function buildFunccallUrl() {
            return buildUrlFromSegments(context.proxyBasePath, [
                'funccall',
                context.tenant,
                context.namespace,
                context.name,
                context.sampleQueryPath,
            ]);
        }

        function updateLatencyDisplays(response) {
            const startDiv = getElement('startDiv');
            const ttftDiv = getElement('ttftDiv');
            const tpsDiv = getElement('tpsDiv');
            const restore = response.headers.get('tcpconn_latency_header');
            const ttft = response.headers.get('ttft_latency_header');

            if (startDiv) {
                startDiv.innerHTML = 'Start Latency: ' + formatLatencyValue(restore) + ' ms <br>';
            }
            if (ttftDiv) {
                ttftDiv.innerHTML = 'Time To First Token: ' + formatLatencyValue(ttft) + ' ms <br>';
            }
            if (tpsDiv && !response.ok) {
                tpsDiv.textContent = '';
            }
        }

        function resetVisualOutputs() {
            const output = getElement('output');
            const debug = getElement('debug');
            const tpsDiv = getElement('tpsDiv');
            const myImage = getElement('myImage');
            const myAudio = getElement('myAudio');

            setElementText(output, '');
            if (debug && typeof debug.value === 'string') {
                debug.value = '';
            }
            if (tpsDiv) {
                tpsDiv.textContent = '';
            }
            if (output) {
                output.hidden = false;
            }
            if (myImage) {
                myImage.style.display = 'none';
                myImage.removeAttribute('src');
            }
            if (myAudio) {
                myAudio.pause();
                myAudio.style.display = 'none';
                myAudio.removeAttribute('src');
            }
        }

        function startRequestUi() {
            const button = getElement('button');
            const cancelBtn = getElement('cancel');
            const processing = getElement('processing');
            const runLabel = getElement('run-label');

            abortController = new AbortController();
            if (cancelBtn) {
                cancelBtn.disabled = false;
            }
            setBusyState(button, processing, runLabel, true);
            notifyStart();
            return abortController.signal;
        }

        function finishRequestUi() {
            const button = getElement('button');
            const cancelBtn = getElement('cancel');
            const processing = getElement('processing');
            const runLabel = getElement('run-label');

            abortController = null;
            if (cancelBtn) {
                cancelBtn.disabled = true;
            }
            setBusyState(button, processing, runLabel, false);
            notifyFinish();
        }

        async function streamOutputText() {
            const output = getElement('output');
            const debug = getElement('debug');
            const prompt = String((getElement('prompt') || {}).value || '');
            const inputUrl = String((getElement('urlInput') || {}).value || '');
            const tpsDiv = getElement('tpsDiv');
            const signal = startRequestUi();

            resetVisualOutputs();

            try {
                const requestMap = cloneMapValue(context.map) || {};
                let body = '';

                if (context.apiType === 'text2text') {
                    requestMap.prompt = prompt;
                    body = JSON.stringify(requestMap);
                } else if (context.apiType === 'image2text') {
                    body = JSON.stringify({
                        model: requestMap.model,
                        messages: [
                            {
                                role: 'user',
                                content: [
                                    { type: 'text', text: prompt },
                                    {
                                        type: 'image_url',
                                        image_url: {
                                            url: inputUrl,
                                        },
                                    },
                                ],
                            },
                        ],
                        max_tokens: requestMap.max_tokens,
                        temperature: requestMap.temperature,
                        stream: true,
                    });
                } else {
                    body = JSON.stringify({
                        model: requestMap.model,
                        messages: [
                            {
                                role: 'user',
                                content: [
                                    { type: 'text', text: prompt },
                                    {
                                        type: 'audio_url',
                                        audio_url: {
                                            url: inputUrl,
                                        },
                                    },
                                ],
                            },
                        ],
                        max_tokens: requestMap.max_tokens,
                        temperature: requestMap.temperature,
                        stream: true,
                    });
                }

                appendElementText(debug, JSON.stringify(requestMap, null, 2));

                const response = await fetch(buildFunccallUrl(), {
                    method: 'POST',
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'X-Inferx-Timeout': '60',
                    },
                    body: body,
                    signal: signal,
                });

                updateLatencyDisplays(response);

                if (!response.ok) {
                    setElementText(output, await response.text());
                    return;
                }

                if (!response.body) {
                    return;
                }

                const reader = response.body.getReader();
                const decoder = new TextDecoder('utf-8');
                const startTime = Date.now();
                let tokenCount = 0;
                let buffer = '';

                while (true) {
                    const result = await reader.read();
                    if (result.done) {
                        break;
                    }

                    buffer += decoder.decode(result.value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    for (const line of lines) {
                        const trimmed = line.trim();
                        if (trimmed === '' || !trimmed.startsWith('data:')) {
                            continue;
                        }

                        const jsonPart = trimmed.replace(/^data:\s*/, '');
                        if (jsonPart === '[DONE]') {
                            return;
                        }

                        try {
                            const parsed = JSON.parse(jsonPart);
                            const content = parsed.choices?.[0]?.delta?.content ?? parsed.choices?.[0]?.text ?? '';
                            if (content) {
                                notifyFirstOutput();
                                appendElementText(output, content);
                                tokenCount += 1;
                                const elapsed = Math.max((Date.now() - startTime) / 1000, 0.001);
                                if (tpsDiv) {
                                    tpsDiv.textContent = 'TPS: ' + (tokenCount / elapsed).toFixed(0) + '     Tokens: ' + tokenCount;
                                }
                            }
                        } catch (error) {
                            console.warn('JSON parse error:', error, 'on line:', jsonPart);
                            buffer = jsonPart + buffer;
                        }
                    }
                }
            } catch (error) {
                if (!(error instanceof DOMException && error.name === 'AbortError')) {
                    console.error('Error fetching inference output:', error);
                }
            } finally {
                finishRequestUi();
            }
        }

        async function streamOutputImage() {
            const output = getElement('output');
            const prompt = String((getElement('prompt') || {}).value || '');
            const signal = startRequestUi();

            resetVisualOutputs();

            try {
                const response = await fetch(new URL(context.text2imgPath, window.location.origin).toString(), {
                    method: 'POST',
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'X-Inferx-Timeout': '60',
                    },
                    body: JSON.stringify({
                        prompt: prompt,
                        tenant: context.tenant,
                        namespace: context.namespace,
                        funcname: context.name,
                    }),
                    signal: signal,
                });

                updateLatencyDisplays(response);

                if (!response.ok) {
                    setElementText(output, await response.text());
                    return;
                }

                const data = await response.json();
                const base64Data = data?.choices?.[0]?.message?.content?.[0]?.image_url?.url;
                if (!base64Data) {
                    throw new Error('Image response was missing image data');
                }

                const image = getElement('myImage');
                if (image) {
                    image.src = base64Data;
                    image.style.display = 'block';
                }
                notifyFirstOutput();
                if (output) {
                    output.hidden = true;
                }
            } catch (error) {
                if (!(error instanceof DOMException && error.name === 'AbortError')) {
                    console.error('Error fetching image output:', error);
                    setElementText(output, String(error && error.message || error));
                }
            } finally {
                finishRequestUi();
            }
        }

        async function streamOutputAudio() {
            const output = getElement('output');
            const prompt = String((getElement('prompt') || {}).value || '');
            const signal = startRequestUi();
            resetVisualOutputs();

            // --- CONFIGURATION ---
            const SOURCE_RATE = 24000; // Updated to your target rate
            const BUFFER_THRESHOLD = 0.03; // 150ms "safety" buffer to prevent stuttering
            // ---------------------

            let leftOverByte = null;
            // Create and resume AudioContext before fetch (user gesture)
            const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            await audioCtx.resume();
            let nextTime = audioCtx.currentTime;  // immediate playback
            let isStarted = false

            try {
                const t0 = performance.now();

                const response = await fetch(
                    new URL(context.text2audioPath, window.location.origin).toString(),
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'X-Inferx-Timeout': '60', 'X-Accel-Buffering': 'no' },
                        body: JSON.stringify({
                            prompt: prompt,
                            tenant: context.tenant,
                            namespace: context.namespace,
                            funcname: context.name,
                            stream: true,
                            response_format: "pcm",
                            voice: 'Vivian',
                        }),
                        signal: signal,
                        priority: 'high',
                        cache: 'no-store'
                    }
                );
                updateLatencyDisplays(response);

                if (!response.ok || !response.body) {
                    setElementText(output, await response.text());
                    return;
                }

                const reader = response.body.getReader();
                notifyFirstOutput();

                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;

                    // Initialize Context on first chunk (browser requirement for user gesture)
                    if (!audioCtx) {
                        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                        nextTime = audioCtx.currentTime + 0.02;
                    }

                    let chunk = value;

                    // Handle Byte Alignment for PCM16 (2 bytes per sample)
                    if (leftOverByte !== null) {
                        const combined = new Uint8Array(chunk.length + 1);
                        combined[0] = leftOverByte;
                        combined.set(chunk, 1);
                        chunk = combined;
                        leftOverByte = null;
                    }

                    if (chunk.byteLength % 2 !== 0) {
                        leftOverByte = chunk[chunk.length - 1];
                        chunk = chunk.slice(0, chunk.length - 1);
                    }

                    if (chunk.byteLength <= 0) continue;

                    // Convert PCM16 to Float32
                    const pcm16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.byteLength / 2);
                    const float32 = new Float32Array(pcm16.length);
                    for (let i = 0; i < pcm16.length; i++) {
                        float32[i] = pcm16[i] / 32768.0;
                    }

                    // Resample
                    const resampled = resampleLinear(float32, SOURCE_RATE, audioCtx.sampleRate);

                    // Create and Schedule Buffer
                    const audioBuffer = audioCtx.createBuffer(1, resampled.length, audioCtx.sampleRate);
                    audioBuffer.copyToChannel(resampled, 0);

                    const source = audioCtx.createBufferSource();
                    source.buffer = audioBuffer;
                    source.connect(audioCtx.destination);

                    // JITTER MANAGEMENT:
                    // If the network fell behind and we are already past 'nextTime', 
                    // reset nextTime to slightly in the future to rebuild the buffer.
                    if (nextTime < audioCtx.currentTime) {
                        nextTime = audioCtx.currentTime + BUFFER_THRESHOLD;
                    }

                    source.start(nextTime);
                    nextTime += audioBuffer.duration;
                }
            } catch (error) {
                if (!(error instanceof DOMException && error.name === 'AbortError')) {
                    console.error('Streaming Error:', error);
                }
            } finally {
                finishRequestUi();
            }
        }

        function resampleLinear(input, srcRate, dstRate) {
            if (srcRate === dstRate) return input;
            const ratio = srcRate / dstRate;
            const newLength = Math.floor(input.length / ratio);
            const output = new Float32Array(newLength);
            for (let i = 0; i < newLength; i++) {
                const pos = i * ratio;
                const idx = Math.floor(pos);
                const frac = pos - idx;
                const s1 = input[idx] || 0;
                const s2 = (idx + 1 < input.length) ? input[idx + 1] : s1;
                output[i] = s1 + (s2 - s1) * frac;
            }
            return output;
        }

        function cancel() {
            if (abortController) {
                abortController.abort();
            }
        }

        async function streamOutput() {
            if (context.apiType === 'text2img') {
                await streamOutputImage();
                return;
            }
            if (context.apiType === 'text2audio') {
                await streamOutputAudio();
                return;
            }
            await streamOutputText();
        }

        return {
            loadImage: loadImage,
            loadAudio: loadAudio,
            streamOutput: streamOutput,
            cancel: cancel,
            isInferenceInFlight: function () {
                return inferenceInFlight;
            },
        };
    }

    window.createInferxInferenceController = createInferenceController;
}());
