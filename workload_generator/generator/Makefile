all: generator 
	
generator:
	$(CC) -I/opt/homebrew/Cellar/gsl/2.7.1/include generator.c -o generator -lgsl -lgslcblas -L/opt/homebrew/Cellar/gsl/2.7.1/lib

clean:
	rm generator
