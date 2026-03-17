"""Project structure visualizer.

Displays the directory tree structure of the microsip_audit_framework project.

Usage:
    python show_structure.py
    python show_structure.py --max-depth 3
    python show_structure.py --include-hidden
"""

import argparse
from pathlib import Path
from typing import List, Set


IGNORE_PATTERNS: Set[str] = {
    '__pycache__',
    '.git',
    '.vscode',
    '.idea',
    'node_modules',
    '.pytest_cache',
    '*.pyc',
    '.DS_Store',
    'venv',
    'env',
}


def should_ignore(path: Path) -> bool:
    """Check if path should be ignored."""
    if path.name in IGNORE_PATTERNS:
        return True
    if path.name.startswith('.') and path.name not in {'.gitignore', '.env.example'}:
        return True
    if path.suffix == '.pyc':
        return True
    return False


def get_tree_lines(
    directory: Path,
    prefix: str = '',
    max_depth: int = None,
    current_depth: int = 0,
    include_hidden: bool = False
) -> List[str]:
    """Generate tree structure lines recursively."""
    if max_depth is not None and current_depth >= max_depth:
        return []
    
    lines = []
    
    try:
        items = sorted(directory.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        
        if not include_hidden:
            items = [item for item in items if not should_ignore(item)]
        
        for i, item in enumerate(items):
            is_last = i == len(items) - 1
            current_prefix = '└── ' if is_last else '├── '
            next_prefix = '    ' if is_last else '│   '
            
            if item.is_dir():
                lines.append(f"{prefix}{current_prefix}{item.name}/")
                
                sub_lines = get_tree_lines(
                    item,
                    prefix + next_prefix,
                    max_depth,
                    current_depth + 1,
                    include_hidden
                )
                lines.extend(sub_lines)
            else:
                lines.append(f"{prefix}{current_prefix}{item.name}")
    
    except PermissionError:
        pass
    
    return lines


def print_structure(
    root_path: Path,
    max_depth: int = None,
    include_hidden: bool = False
) -> None:
    """Print project structure."""
    print(f"\n{root_path.name}/")
    
    lines = get_tree_lines(
        root_path,
        max_depth=max_depth,
        include_hidden=include_hidden
    )
    
    for line in lines:
        print(line)
    
    print(f"\nTotal items: {len(lines)}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Display project directory structure',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  Show full structure:
    python show_structure.py
  
  Limit depth to 2 levels:
    python show_structure.py --max-depth 2
  
  Include hidden files:
    python show_structure.py --include-hidden
        '''
    )
    
    parser.add_argument(
        '--max-depth',
        type=int,
        help='Maximum depth to display (default: unlimited)'
    )
    
    parser.add_argument(
        '--include-hidden',
        action='store_true',
        help='Include hidden files and ignored patterns'
    )
    
    parser.add_argument(
        '--path',
        type=str,
        default='.',
        help='Root path to display (default: current directory)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    root_path = Path(args.path).resolve()
    
    if not root_path.exists():
        print(f"Error: Path does not exist: {root_path}")
        return 1
    
    if not root_path.is_dir():
        print(f"Error: Path is not a directory: {root_path}")
        return 1
    
    print("=" * 70)
    print("PROJECT STRUCTURE")
    print("=" * 70)
    
    print_structure(
        root_path,
        max_depth=args.max_depth,
        include_hidden=args.include_hidden
    )
    
    print("\n" + "=" * 70)
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())