pdf:
	pandoc tutorial.md -o tutorial.pdf

slides:
	pandoc slides.md --toc --toc-depth=3 -t beamer -o tutorial_slides.pdf --slide-level=2 --template=default_mod.latex

src_pdf:
	pandoc src_example/README.md -o src_example/README.pdf
