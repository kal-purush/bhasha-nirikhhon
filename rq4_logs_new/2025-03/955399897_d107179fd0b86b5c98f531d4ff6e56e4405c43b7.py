import ast
import json
import csv
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Union, Set, Optional, Any, Tuple
import os
import re
from functools import lru_cache

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("repo_analyzer")

@dataclass
class FunctionInfo:
    """Información detallada sobre una función."""
    name: str
    parameters: List[str]
    decorators: List[str]
    line_count: int
    complexity: int
    docstring: Optional[str]

@dataclass
class ClassInfo:
    """Información detallada sobre una clase."""
    name: str
    methods: List[str]
    attributes: List[str]
    parent_classes: List[str]
    decorators: List[str]
    line_count: int
    docstring: Optional[str]

@dataclass
class FileAnalysis:
    """Resultado del análisis de un archivo."""
    classes: List[ClassInfo]
    functions: List[FunctionInfo]
    imports: List[str]
    code_lines: int
    comment_lines: int
    blank_lines: int
    total_lines: int
    errors: List[str]

class RepoAnalyzer:
    def __init__(self, repo_path: Union[str, Path], config: Optional[Dict[str, Any]] = None):
        """
        Inicializa el analizador de repositorios.
        
        Args:
            repo_path: Ruta al repositorio a analizar
            config: Configuración opcional para el analizador
        """
        self.repo_path = Path(repo_path)
        self.config = config or self._default_config()
        self.results = {}
        self.start_time = None
        self._setup_exclude_patterns()
        
    def _default_config(self) -> Dict[str, Any]:
        """Configuración predeterminada del analizador."""
        return {
            "exclude_patterns": [
                r"venv", r"\.venv", r"__pycache__", r"\.git", 
                r"\.pytest_cache", r"build", r"dist", r"\.eggs",
                r"node_modules", r"\.idea", r"\.vs"
            ],
            "exclude_files": [r"__init__\.py$"],
            "max_workers": os.cpu_count(),
            "batch_size": 100,  # Número de archivos a procesar por lote
            "complexity_threshold": 10,  # Complejidad ciclomática considerada alta
        }
    
    def _setup_exclude_patterns(self):
        """Compila los patrones de exclusión para mayor eficiencia."""
        self.exclude_dir_patterns = [re.compile(pattern) for pattern in self.config["exclude_patterns"]]
        self.exclude_file_patterns = [re.compile(pattern) for pattern in self.config["exclude_files"]]
    
    def should_exclude(self, path: Path) -> bool:
        """Determina si un archivo o directorio debe ser excluido del análisis."""
        # Verificar si alguna parte de la ruta coincide con patrones de exclusión
        for part in path.parts:
            for pattern in self.exclude_dir_patterns:
                if pattern.search(part):
                    return True
                    
        # Verificar si el nombre del archivo coincide con patrones de exclusión de archivos
        if path.is_file():
            for pattern in self.exclude_file_patterns:
                if pattern.search(path.name):
                    return True
        
        return False
    
    def get_python_files(self) -> List[Path]:
        """Encuentra todos los archivos Python en el repositorio que no deben ser excluidos."""
        python_files = []
        for path in self.repo_path.rglob("*.py"):
            if not self.should_exclude(path):
                python_files.append(path)
        return python_files
    
    def analyze_repo(self) -> Dict[str, FileAnalysis]:
        """
        Analiza todos los archivos Python válidos en el repositorio.
        Utiliza procesamiento paralelo para mejorar el rendimiento.
        """
        self.start_time = time.time()
        python_files = self.get_python_files()
        total_files = len(python_files)
        
        logger.info(f"Encontrados {total_files} archivos Python para analizar")
        
        # Analizar archivos en lotes para evitar sobrecarga de memoria
        self.results = {}
        batches = [python_files[i:i + self.config["batch_size"]] 
                  for i in range(0, len(python_files), self.config["batch_size"])]
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"Procesando lote {batch_idx+1}/{len(batches)} ({len(batch)} archivos)")
            self._process_batch(batch, batch_idx, len(batches))
            
        elapsed = time.time() - self.start_time
        logger.info(f"Análisis completado en {elapsed:.2f} segundos")
        
        return self.results
    
    def _process_batch(self, files: List[Path], batch_idx: int, total_batches: int):
        """Procesa un lote de archivos en paralelo."""
        with ProcessPoolExecutor(max_workers=self.config["max_workers"]) as executor:
            future_to_file = {
                executor.submit(self._analyze_file_wrapper, file): file 
                for file in files
            }
            
            completed = 0
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    rel_path = str(file_path.relative_to(self.repo_path))
                    result = future.result()
                    self.results[rel_path] = result
                except Exception as e:
                    logger.error(f"Error procesando {file_path}: {str(e)}")
                    rel_path = str(file_path.relative_to(self.repo_path))
                    self.results[rel_path] = FileAnalysis(
                        classes=[], functions=[], imports=[], 
                        code_lines=0, comment_lines=0, blank_lines=0, total_lines=0,
                        errors=[f"Error de procesamiento: {str(e)}"]
                    )
                
                completed += 1
                if completed % 10 == 0 or completed == len(files):
                    progress = (batch_idx * self.config["batch_size"] + completed) / (total_batches * self.config["batch_size"])
                    elapsed = time.time() - self.start_time
                    eta = (elapsed / progress) - elapsed if progress > 0 else 0
                    logger.info(f"Progreso: {progress*100:.1f}% - ETA: {eta:.1f}s")
    
    def _analyze_file_wrapper(self, file_path: Path) -> FileAnalysis:
        """Wrapper para análisis de archivos para manejo de excepciones en subprocesos."""
        try:
            return self.analyze_file(file_path)
        except Exception as e:
            logger.error(f"Error analizando {file_path}: {str(e)}")
            return FileAnalysis(
                classes=[], functions=[], imports=[], 
                code_lines=0, comment_lines=0, blank_lines=0, total_lines=0,
                errors=[f"Error: {str(e)}"]
            )
    
    def analyze_file(self, file_path: Path) -> FileAnalysis:
        """
        Analiza un archivo Python en profundidad.
        Extrae información detallada sobre clases, funciones, métricas de código, etc.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                source = f.read()
            
            # Análisis de líneas
            lines = source.split("\n")
            blank_lines = sum(1 for line in lines if not line.strip())
            comment_lines = sum(1 for line in lines if line.strip().startswith("#"))
            
            # Análisis AST
            tree = ast.parse(source)
            visitor = EnhancedAstVisitor(source)
            visitor.visit(tree)
            
            return FileAnalysis(
                classes=visitor.classes,
                functions=visitor.functions,
                imports=visitor.imports,
                code_lines=len(lines) - blank_lines - comment_lines,
                comment_lines=comment_lines,
                blank_lines=blank_lines,
                total_lines=len(lines),
                errors=[]
            )
        except Exception as e:
            return FileAnalysis(
                classes=[], functions=[], imports=[], 
                code_lines=0, comment_lines=0, blank_lines=0, total_lines=0,
                errors=[f"Error analizando archivo: {str(e)}"]
            )
    
    def export_results(self, format_type: str = "json", output_path: Optional[Path] = None):
        """
        Exporta los resultados del análisis a un archivo.
        
        Args:
            format_type: Formato de exportación ('json' o 'csv')
            output_path: Ruta del archivo de salida
        """
        if not self.results:
            logger.warning("No hay resultados para exportar.")
            return
        
        if output_path is None:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            output_path = self.repo_path / f"analysis_report_{timestamp}.{format_type}"
        
        output_path = Path(output_path)
        
        # Convertir resultados a un formato serializable
        serializable_results = {}
        for file_path, analysis in self.results.items():
            serializable_results[file_path] = self._make_serializable(analysis)
        
        if format_type == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(serializable_results, f, indent=2)
        elif format_type == "csv":
            self._export_to_csv(serializable_results, output_path)
        else:
            raise ValueError(f"Formato de exportación no soportado: {format_type}")
        
        logger.info(f"Resultados exportados a {output_path}")
    
    def _make_serializable(self, obj: Any) -> Any:
        """Convierte objetos complejos a formatos serializables."""
        if hasattr(obj, "__dataclass_fields__"):  # Es un dataclass
            return {k: self._make_serializable(v) for k, v in asdict(obj).items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        else:
            return obj
    
    def _export_to_csv(self, results: Dict[str, Dict], output_path: Path):
        """Exporta los resultados a formato CSV (simplificado)."""
        # Crear una versión plana de los datos para CSV
        flat_data = []
        for file_path, analysis in results.items():
            row = {
                "file_path": file_path,
                "total_lines": analysis.get("total_lines", 0),
                "code_lines": analysis.get("code_lines", 0),
                "comment_lines": analysis.get("comment_lines", 0),
                "blank_lines": analysis.get("blank_lines", 0),
                "class_count": len(analysis.get("classes", [])),
                "function_count": len(analysis.get('functions', [])) + sum(len(cls.get('methods', [])) for cls in analysis.get('classes', [])),
                "import_count": len(analysis.get("imports", [])),
                "has_errors": bool(analysis.get("errors", []))
            }
            flat_data.append(row)
        
        # Escribir al archivo CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            if flat_data:
                writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
                writer.writeheader()
                writer.writerows(flat_data)
    
    def generate_summary(self) -> Dict[str, Any]:
        """Genera un resumen estadístico del análisis."""
        if not self.results:
            return {"error": "No hay resultados para generar resumen"}
        
        total_files = len(self.results)
        total_lines = sum(r.total_lines for r in self.results.values())
        total_code_lines = sum(r.code_lines for r in self.results.values())
        total_classes = sum(len(r.classes) for r in self.results.values())
        total_functions = sum(len(r.functions) for r in self.results.values())
        
        # Encontrar archivos con mayor complejidad
        complex_functions = []
        for file_path, analysis in self.results.items():
            for func in analysis.functions:
                if func.complexity > self.config["complexity_threshold"]:
                    complex_functions.append({
                        "file": file_path,
                        "function": func.name,
                        "complexity": func.complexity
                    })
        
        # Ordenar por complejidad descendente
        complex_functions.sort(key=lambda x: x["complexity"], reverse=True)
        
        return {
            "total_files": total_files,
            "total_lines": total_lines,
            "total_code_lines": total_code_lines,
            "total_classes": total_classes,
            "total_functions": total_functions + sum(len(c.methods) for r in self.results.values() for c in r.classes),
            "complex_functions": complex_functions[:10],  # Top 10 funciones más complejas
            "avg_file_size": total_lines / total_files if total_files else 0,
            "code_to_comment_ratio": total_code_lines / (total_lines - total_code_lines) if (total_lines - total_code_lines) else float("inf")
        }


class EnhancedAstVisitor(ast.NodeVisitor):
    """Enhanced Visitor para recorrer el AST y extraer información detallada."""
    
    def __init__(self, source_code: str):
        self.source_code = source_code
        self.source_lines = source_code.split('\n')
        self.classes = []
        self.functions = []
        self.imports = []
        self.current_class = None
    
    def visit_ClassDef(self, node):
        """Visita definiciones de clase y extrae información detallada."""
        try:
            # Obtener clases padre con manejo de herencia más robusto
            parent_classes = []
            for base in node.bases:
                try:
                    if isinstance(base, ast.Name):
                        parent_classes.append(base.id)
                    elif isinstance(base, ast.Attribute):
                        parent_classes.append(self._get_attribute_path(base))
                    elif isinstance(base, ast.Call):
                        # Manejo de herencia con inicialización compleja
                        parent_classes.append(self._get_attribute_path(base.func))
                except Exception as e:
                    parent_classes.append(str(e))
            
            # Extraer decoradores
            decorators = []
            for decorator in node.decorator_list:
                try:
                    decorator_str = self._get_decorator_representation(decorator)
                    decorators.append(decorator_str)
                except Exception as e:
                    decorators.append(str(e))
            
            # Extraer docstring
            docstring = ast.get_docstring(node)
            
            # Calcular líneas de código
            line_count = node.end_lineno - node.lineno + 1 if hasattr(node, 'end_lineno') else 0
            
            # Guardar clase actual para procesar sus métodos
            old_class = self.current_class
            self.current_class = node.name
            
            # Procesar el cuerpo de la clase para extraer atributos
            attributes = []
            for item in node.body:
                if isinstance(item, (ast.Assign, ast.AnnAssign)):
                    # Manejar asignaciones con anotaciones de tipo
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                attributes.append(target.id)
                    elif isinstance(item, ast.AnnAssign):
                        if isinstance(item.target, ast.Name):
                            attributes.append(item.target.id)
            
            # Crear objeto ClassInfo
            class_info = ClassInfo(
                name=node.name,
                methods=[],  # Se llenará al visitar las definiciones de métodos
                attributes=attributes,
                parent_classes=parent_classes,
                decorators=decorators,
                line_count=line_count,
                docstring=docstring
            )
            
            self.classes.append(class_info)
            
            # Visitar recursivamente el cuerpo de la clase
            for item in node.body:
                self.visit(item)
            
            self.current_class = old_class
        except Exception as e:
            logger.error(f"Error procesando clase {node.name}: {e}")
    
    def visit_FunctionDef(self, node):
        """Visita definiciones de funciones con mayor robustez."""
        try:
            # Extraer nombres de parámetros con manejo de tipos más complejos
            parameters = []
            for arg in node.args.args:
                param_name = arg.arg
                # Intentar extraer información de anotaciones de tipo
                if arg.annotation:
                    try:
                        type_hint = self._get_attribute_path(arg.annotation)
                        parameters.append(f"{param_name}: {type_hint}")
                    except Exception:
                        parameters.append(param_name)
                else:
                    parameters.append(param_name)
            
            # Manejar argumentos con valores por defecto
            if node.args.defaults:
                for i, default in enumerate(node.args.defaults):
                    try:
                        default_value = self._source_segment(default)
                        parameters[-(len(node.args.defaults) - i)] += f" = {default_value}"
                    except Exception:
                        pass
            
            # Extraer decoradores con mayor detalle
            decorators = []
            for decorator in node.decorator_list:
                try:
                    decorator_str = self._get_decorator_representation(decorator)
                    decorators.append(decorator_str)
                except Exception as e:
                    decorators.append(str(e))
            
            # Extraer docstring
            docstring = ast.get_docstring(node)
            
            # Calcular líneas de código
            line_count = node.end_lineno - node.lineno + 1 if hasattr(node, 'end_lineno') else 0
            
            # Calcular complejidad ciclomática (aproximada)
            complexity = self._calculate_complexity(node)
            
            # Crear objeto FunctionInfo
            func_info = FunctionInfo(
                name=node.name,
                parameters=parameters,
                decorators=decorators,
                line_count=line_count,
                complexity=complexity,
                docstring=docstring
            )
            
            # Si es un método de una clase, agregarlo a la clase correspondiente
            if self.current_class:
                for class_info in self.classes:
                    if class_info.name == self.current_class:
                        class_info.methods.append(node.name)
                        break
            else:
                self.functions.append(func_info)
            
            # Visitar recursivamente el cuerpo de la función
            for item in node.body:
                self.visit(item)
        except Exception as e:
            logger.error(f"Error procesando función {node.name}: {e}")
    
    def visit_Import(self, node):
        """Visita declaraciones de importación."""
        for alias in node.names:
            self.imports.append(alias.name)
    
    def visit_ImportFrom(self, node):
        """Visita declaraciones de importación del tipo 'from X import Y'."""
        if node.module:
            for alias in node.names:
                self.imports.append(f"{node.module}.{alias.name}")
    
    def _calculate_complexity(self, node) -> int:
        """
        Calcula una aproximación de la complejidad ciclomática.
        Cuenta el número de ramas en el flujo de control.
        """
        complexity = 1  # Base complexity
        
        class ComplexityVisitor(ast.NodeVisitor):
            def __init__(self):
                self.complexity = 0
            
            def visit_If(self, node):
                self.complexity += 1
                # Agregar complejidad para cada condición elif
                if node.orelse and isinstance(node.orelse[0], ast.If):
                    self.complexity += len([n for n in node.orelse if isinstance(n, ast.If)])
                self.generic_visit(node)
            
            def visit_For(self, node):
                self.complexity += 1
                self.generic_visit(node)
            
            def visit_While(self, node):
                self.complexity += 1
                self.generic_visit(node)
            
            def visit_Try(self, node):
                # Agregar complejidad para cada handler de excepción
                self.complexity += max(1, len(node.handlers)) 
                self.generic_visit(node)
            
            def visit_BoolOp(self, node):
                # Agregar complejidad por operadores booleanos
                if isinstance(node.op, (ast.And, ast.Or)):
                    self.complexity += len(node.values) - 1
                self.generic_visit(node)
            
            def visit_ListComp(self, node):
                # Agregar complejidad por comprensiones de lista con múltiples generadores
                self.complexity += len(node.generators)
                self.generic_visit(node)
            
            def visit_DictComp(self, node):
                # Agregar complejidad por comprensiones de diccionario
                self.complexity += len(node.generators)
                self.generic_visit(node)
        
        visitor = ComplexityVisitor()
        visitor.visit(node)
        complexity += visitor.complexity
        
        return complexity
    
    def _get_attribute_path(self, node) -> str:
        """Obtiene la representación en cadena de un nodo de atributo."""
        try:
            if isinstance(node, ast.Attribute):
                return f"{self._get_attribute_path(node.value)}.{node.attr}"
            elif isinstance(node, ast.Name):
                return node.id
            elif isinstance(node, ast.Call):
                return self._get_attribute_path(node.func)
            elif isinstance(node, ast.Subscript):
                # Manejo de anotaciones de tipo con indexación
                return f"{self._get_attribute_path(node.value)}[{self._get_attribute_path(node.slice)}]"
            elif isinstance(node, ast.Index):
                return self._get_attribute_path(node.value)
            else:
                return str(type(node).__name__)
        except Exception as e:
            return f"Error({str(e)})"
    
    def _get_decorator_representation(self, decorator):
        """Obtiene una representación más detallada de los decoradores."""
        try:
            if isinstance(decorator, ast.Call):
                # Para decoradores con argumentos
                func = self._get_attribute_path(decorator.func)
                args = [self._source_segment(arg) for arg in decorator.args]
                return f"{func}({', '.join(args)})"
            else:
                # Para decoradores simples
                return self._get_attribute_path(decorator.value)
        except Exception as e:
            return f"Error({str(e)})"
    
    def _source_segment(self, node):
        """Extrae el segmento de código fuente para un nodo."""
        try:
            return ast.unparse(node)
        except Exception:
            return str(node)


def analyze_repository(repo_path: Union[str, Path], config: Optional[Dict] = None) -> Dict:
    """
    Función principal para analizar un repositorio.
    
    Args:
        repo_path: Ruta al repositorio
        config: Configuración opcional
        
    Returns:
        Diccionario con resultados y resumen del análisis
    """
    analyzer = RepoAnalyzer(repo_path, config)
    results = analyzer.analyze_repo()
    summary = analyzer.generate_summary()
    
    # Exportar resultados automáticamente
    analyzer.export_results("json")
    
    return {
        "summary": summary,
        "results": results
    }


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Analizador de repositorios Python")
    parser.add_argument("repo_path", help="Ruta al repositorio a analizar")
    parser.add_argument("--output", "-o", help="Ruta para guardar el reporte")
    parser.add_argument("--format", "-f", choices=["json", "csv"], default="json", help="Formato de exportación")
    parser.add_argument("--workers", "-w", type=int, default=os.cpu_count(), help="Número de workers para procesamiento paralelo")

    args = parser.parse_args()

    # Forzar que sea Path
    repo_path = Path(args.repo_path)

    analyzer = RepoAnalyzer(repo_path)
    analyzer.config["max_workers"] = args.workers

    analyzer.analyze_repo()
    summary = analyzer.generate_summary()

    print("\n--- RESUMEN DEL ANÁLISIS ---")
    print(f"Total de archivos analizados: {summary['total_files']}")
    print(f"Total de líneas de código: {summary['total_code_lines']}")
    print(f"Total de clases: {summary['total_classes']}")
    print(f"Total de funciones: {summary['total_functions']}")
    print(f"Tamaño promedio de archivo: {summary['avg_file_size']:.1f} líneas")

    if summary.get('complex_functions'):
        print("\nFunciones más complejas:")
        for func in summary['complex_functions'][:5]:
            print(f"  - {func['file']}::{func['function']} (Complejidad: {func['complexity']})")

    if args.output:
        analyzer.export_results(args.format, args.output)
    else:
        analyzer.export_results(args.format)