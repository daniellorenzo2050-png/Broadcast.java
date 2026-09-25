import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

public class Broadcast implements WebSocket.Listener {
    
    private WebSocket clientSocket;
    private final String serverUri;

    public Broadcast(String serverUri) {
        this.serverUri = serverUri;
    }

    /**
     * Conecta de verdade ao servidor WebSocket do WebBroadcast
     */
    public void connect() {
        System.out.println("[WebBroadcast] Conectando ao servidor: " + serverUri);
        HttpClient client = HttpClient.newHttpClient();
        client.newWebSocketBuilder()
              .buildAsync(URI.create(serverUri), this)
              .thenAccept(ws -> {
                  this.clientSocket = ws;
                  System.out.println("[WebBroadcast] Conexão WebSocket estabelecida com sucesso!");
              })
              .exceptionally(ex -> {
                  System.err.println("[WebBroadcast] Erro ao conectar: " + ex.getMessage());
                  return null;
              })
              .join();
    }

    /**
     * Codifica uma mensagem de texto usando exclusivamente o formato HC-8 (Binário Puro de 8 bits)
     */
    private byte[] encodeHC8(String text) {
        if (text == null) return new byte[0];
        // Converte diretamente para o array de bytes brutos de 8 bits do HC-8
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodifica o HC-8 Binário Puro de 8 bits de volta para visualização
     */
    private String decodeHC8(byte[] hc8Bytes) {
        if (hc8Bytes == null || hc8Bytes.length == 0) return "";
        return new String(hc8Bytes, StandardCharsets.UTF_8);
    }

    /**
     * Envia um pacote real via WebBroadcast utilizando datagramas binários com HC-8
     * 
     * @param suiteId      Identificador da suite (uint16)
     * @param isCritical   Se true, nível Grave (0x02); se false, Severo (0x01)
     * @param timestampMs  Epoch em milissegundos (uint64)
     * @param metricValue  Valor métrico associado (float32)
     * @param rawMessage   Mensagem a ser convertida por HC-8 (8-bit Raw Binary)
     */
    public void sendWebBroadcastAlert(int suiteId, boolean isCritical, long timestampMs, float metricValue, String rawMessage) {
        if (clientSocket == null || causasFechado()) {
            System.err.println("[WebBroadcast] Erro: Socket não está conectado.");
            return;
        }

        // 1. Processa o texto usando o padrão HC-8 de 8 bits
        byte[] hc8PayloadBytes = encodeHC8(rawMessage);
        
        // 2. Calcula o tamanho total do datagrama binário
        // Header fixo = 24 bytes (4 + 1 + 1 + 2 + 8 + 4 + 4)
        int totalLength = 24 + hc8PayloadBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.order(ByteOrder.BIG_ENDIAN); // Big-Endian padrão para rede

        // 3. Montagem do Datagrama WBP v1
        buffer.putInt(0x57425001);                    // Magic Number 'WBP1' (uint32)
        buffer.put((byte) 0x15);                      // Packet Type: Alerta de Sistema (uint8)
        buffer.put((byte) (isCritical ? 0x02 : 0x01)); // Alert Level: Grave(02) ou Severo(01) (uint8)
        buffer.putShort((short) suiteId);             // Suite ID (uint16)
        buffer.putLong(timestampMs);                  // Timestamp MS (uint64)
        buffer.putFloat(metricValue);                 // Metric Value (float32)
        buffer.putInt(hc8PayloadBytes.length);        // Payload Length do HC-8 (uint32)
        buffer.put(hc8PayloadBytes);                  // Payload HC-8 (8-bit Raw Binary Data)

        // 4. Envia o binário pelo WebSocket
        clientSocket.sendBinary(buffer.flip(), true);
        System.out.printf("[WebBroadcast] Alerta enviado para Suite %d | Nível: %s%n", 
                suiteId, (isCritical ? "GRAVE" : "SEVERO"));
    }

    private boolean causasFechado() {
        return clientSocket.isOutputClosed() || clientSocket.isInputClosed();
    }

    /**
     * Recebe pacotes binários vindos do servidor WebBroadcast
     */
    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        data.order(ByteOrder.BIG_ENDIAN);

        try {
            if (data.remaining() < 24) {
                System.err.println("[WebBroadcast] Pacote binário muito curto recebido.");
                return WebSocket.Listener.super.onBinary(webSocket, data, last);
            }

            // Leitura do Header do Datagrama
            int magic = data.getInt();
            if (magic != 0x57425001) {
                System.err.println("[WebBroadcast] Magic Number inválido recebido!");
                return WebSocket.Listener.super.onBinary(webSocket, data, last);
            }

            byte packetType = data.get();
            byte alertLevel = data.get();
            int suiteId = data.getShort() & 0xFFFF; // unsigned short
            long timestamp = data.getLong();
            float metric = data.getFloat();
            int payloadLen = data.getInt();

            if (data.remaining() < payloadLen) {
                System.err.println("[WebBroadcast] Tamanho do payload HC-8 corrompido ou incompleto.");
                return WebSocket.Listener.super.onBinary(webSocket, data, last);
            }

            // Extração do payload binário puro HC-8 de 8 bits
            byte[] hc8Bytes = new byte[payloadLen];
            data.get(hc8Bytes);
            
            // Decodificação do HC-8 para leitura do alerta
            String message = decodeHC8(hc8Bytes);
            String levelStr = (alertLevel == 0x02) ? "GRAVE" : "SEVERO";

            System.out.println("==================================================");
            System.out.printf("[RECEBIDO WBP] Suite: %d | Nível: %s%n", suiteId, levelStr);
            System.out.printf("Tempo (uint64): %d ms | Métrica (float32): %.2f%n", timestamp, metric);
            System.out.printf("Payload HC-8 (8-bit): %s%n", message);
            System.out.println("==================================================");

        } catch (Exception e) {
            System.err.println("[WebBroadcast] Erro ao processar datagrama binário: " + e.getMessage());
        }

        webSocket.request(1);
        return WebSocket.Listener.super.onBinary(webSocket, data, last);
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        System.out.println("[WebBroadcast] Canal aberto e pronto para transmissão.");
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.out.printf("[WebBroadcast] Conexão encerrada. Código: %d, Razão: %s%n", statusCode, reason);
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.err.println("[WebBroadcast] Erro na conexão WebSocket: " + error.getMessage());
    }
}
