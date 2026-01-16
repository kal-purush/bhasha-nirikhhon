/**
 * SERVER DESCRIPTION
 *
 * This package contains all the classes from the server side of the Gunfire Locator System (GLS).
 *
 * The main class of the server is the {@link com.hugovs.gls.receiver.AudioServer}. The {@link com.hugovs.gls.receiver.AudioServer}
 * is the one that creates the server and hold its extensions.
 *
 *
 * AUDIO SERVER EXTENSION
 *
 * The server are designed to be complemented with {@link com.hugovs.gls.receiver.AudioServerExtension}.
 * Any extra functionality is created by creating a new extension.
 *
 * All the extensions runs in sequence.
 * If your extension is heavy on resources, it's recommended that you create a new thread to process the data.
 *
 * On your extension you can implement {@link com.hugovs.gls.receiver.DataListener} to perform an action every time a
 * new set of samples arrives. You can also implement {@link com.hugovs.gls.receiver.DataFilter} to filter the sound,
 * i. e. remove noise.
 */
package com.hugovs.gls;