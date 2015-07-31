package com.workerbee.annotationprocessor;

import com.workerbee.annotation.WBDatabase;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.util.Set;

@SupportedAnnotationTypes("com.workerbee.annotation.WBDatabase")
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class WBDatabaseProcessor extends AbstractProcessor {
  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    for (Element element : roundEnv.getElementsAnnotatedWith(WBDatabase.class)) {
      String message = "annotation found in " + element.getSimpleName();

      processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, message);
    }

    return true;
  }
}
