/*
 * Just plugin - see how IntellijIdea improving your performance!
 * Copyright 2016 Svitkov Sergey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handlers;

import com.intellij.codeInsight.editorActions.BackspaceHandlerDelegate;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.fileEditor.FileDocumentManager;
import com.intellij.psi.PsiFile;
import stuff.Counter;

public class BackspaceHandler extends BackspaceHandlerDelegate{

	@Override
	public void beforeCharDeleted(char c, PsiFile file, Editor editor) {

	}

	@Override
	public boolean charDeleted(char c, PsiFile file, Editor editor) {
		Counter counter = Counter.getInstance();
		Document currentDocument = editor.getDocument();
		String openedFileName = FileDocumentManager.getInstance().getFile(currentDocument).getPresentableName();
		ApplicationManager.getApplication().runReadAction(() -> counter.decrement(openedFileName));
		return false;
	}
}