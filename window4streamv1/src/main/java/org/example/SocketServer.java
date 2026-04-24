package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServer {

    public static void main(String[] args) {

        ServerSocket serverSocket = null;

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        try {

            serverSocket = new ServerSocket(9999);

            System.out.println("Listening 9999...");

            while (true) {

                Socket clientSocket = serverSocket.accept();

                executorService.execute(new Runnable() {
                    @Override
                    public void run() {

                        BufferedReader bufferedReader = null;
                        BufferedWriter bufferedWriter = null;

                        try {

                            bufferedReader = new BufferedReader(new InputStreamReader(System.in));
                            bufferedWriter = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

                            while (true) {
                                String line = bufferedReader.readLine();
                                bufferedWriter.write(line);
                                bufferedWriter.newLine();
                                bufferedWriter.flush();
                            }

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            if (bufferedReader != null) {
                                try {
                                    bufferedReader.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            if (bufferedWriter != null) {
                                try {
                                    bufferedWriter.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                });
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
