package edu.cmu.cs.cs214.chat.server;

import edu.cmu.cs.cs214.chat.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Chat server that uses sockets to connect clients
 *
 * @author tsun
 */
public class ChatServerImpl extends Thread implements ChatServer {
  private static final int DEFAULT_PORT = 15214;
  private static final String TAG = "SERVER";
  private static final int POOL_SIZE =
      Runtime.getRuntime().availableProcessors();
  private int port;
  private final ExecutorService mExecutor;
  private static List<Socket> clients =
      Collections.synchronizedList(new ArrayList<Socket>());
  private static List<Message> messages =
      Collections.synchronizedList(new ArrayList<Message>());

  /**
   * Constructs a ChatServerImpl on the given port
   * 
   * @param serverPort
   *          Port to run the server on
   */
  public ChatServerImpl(final int serverPort) {
    port = serverPort;
    mExecutor = Executors.newFixedThreadPool(POOL_SIZE);
  }

  @Override
  public void run() {
    try {
      ServerSocket serverSocket = null;
      try {
        serverSocket = new ServerSocket(port);
      } catch (final IOException e) {
        Log.e(TAG, "Could not open server socket on port " + port + ".", e);
        return;
      }

      Log.i(TAG, "Listening for incoming commands on port " + port + ".");

      while (true) {
        try {
          final Socket clientSocket = serverSocket.accept();
          Log.i(TAG, String.format("Got connection from %s:%s",
              clientSocket.getRemoteSocketAddress(), clientSocket.getPort()));
          clients.add(clientSocket);
          mExecutor.execute(new ClientHandler(clientSocket));
        } catch (final IOException e) {
          Log.e(TAG, "Error while listening for incoming connections.", e);
          break;
        }
      }

      Log.i(TAG, "Shutting down...");

      try {
        serverSocket.close();
      } catch (final IOException e) {
        Log.e(TAG, "Unable to close connection");
        // Ignore because we're about to exit anyway.
      }
    } finally {
      mExecutor.shutdown();
    }
  }

  @Override
  public void startServer() {
    start();
  }

  @Override
  public int getNumClients() {
    return clients.size();
  }

  @Override
  public void setPort(final int serverPort) {
    port = serverPort;
  }

  @Override
  public void stopServer() {
    for (final Socket s : clients) {
      if (s != null && !s.isClosed()) {
        try {
          s.close();
        } catch (final IOException e) {
          Log.e(TAG, "Unable to close connection to client.");
        }
      }
    }
    mExecutor.shutdown();
  }

  @Override
  public ArrayList<Message> getMessages() {
    return (ArrayList<Message>) Collections.unmodifiableList(messages);
  }

  /**
   * Handler for every client connection to the server.
   * 
   * @author tsun
   */
  private static class ClientHandler implements Runnable {
    private final Socket socket;

    public ClientHandler(final Socket clientSocket) {
      socket = clientSocket;
    }

    @Override
    public void run() {
      try {
        final ObjectInputStream in =
            new ObjectInputStream(socket.getInputStream());
        while (true) {
          final Message msg = (Message) in.readObject();
          onNewMessage(socket, msg);
        }
      } catch (final IOException e) {
        Log.e(TAG, String.format("Connection lost from client: %s",
            socket.getRemoteSocketAddress()));
        clients.remove(socket);
      } catch (final ClassNotFoundException e) {
        Log.e(TAG, "Received invalid task from client.");
      } finally {
        try {
          socket.close();
        } catch (final IOException e) {
          Log.e(TAG, "Unable to close connection");
        }
      }
    }

    /**
     * Callback for when a message is received by the server. Notifies all
     * clients about the new message received
     * 
     * @param from
     *          Socket where the new message originated
     * @param msg
     *          Message sent by the client
     */
    private void onNewMessage(final Socket from, final Message msg) {
      // TODO: Add the server timestamp to the message received. Note:
      // Message#setServerTimestamp was created for you in the Message
      // class.

      // Synchronize because we are iterating through all clients in a
      // thread
      synchronized (clients) {
        for (final Socket s : clients) {
          try {
            final ObjectOutputStream out =
                new ObjectOutputStream(s.getOutputStream());
            out.writeObject(msg);
          } catch (final IOException e) {
            Log.e(TAG, "Unable to send message to client.");
          }
        }
      }
    }

  }

  /**
   * Runs the chat master server.
   * 
   * @param args
   *          Command line arguments
   */
  public static void main(final String[] args) {
    ChatServer server = null;
    if (args.length > 0) {
      try {
        server = new ChatServerImpl(Integer.parseInt(args[0]));
      } catch (final NumberFormatException e) {
        printHelp();
        System.exit(1);
      }
    } else {
      server = new ChatServerImpl(DEFAULT_PORT);
    }
    server.startServer();
  }

  private static void printHelp() {
    System.err.println("Usage: ./server [PORT]");
  }

}
