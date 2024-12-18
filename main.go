package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type Movie struct {
	ID          int       `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	ReleaseYear int       `json:"release_year"`
	Poster      string    `json:"poster"`
	Director    *Director `json:"director,omitempty"`
}

type Director struct {
	ID        int    `json:"id,omitempty"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

var db *sql.DB

// Connect to the PostgreSQL database
func connectToDB() {
	var err error
	connStr := "user=rajneeshverma dbname=movies_db sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Could not connect to the database: ", err)
	}
	log.Println("Successfully connected to database")
}

func createMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var movie Movie
	err := json.NewDecoder(r.Body).Decode(&movie)
	if err != nil {
		log.Printf("Decode error: %v", err)
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Check if the director exists, if not, insert the director
	var directorID int
	err = db.QueryRow(`
        SELECT id FROM directors WHERE first_name = $1 AND last_name = $2
    `, movie.Director.FirstName, movie.Director.LastName).Scan(&directorID)

	// If the director doesn't exist, insert the director and get the ID
	if err != nil {
		if err == sql.ErrNoRows {
			err = db.QueryRow(`
                INSERT INTO directors (first_name, last_name)
                VALUES ($1, $2)
                RETURNING id
            `, movie.Director.FirstName, movie.Director.LastName).Scan(&directorID)
			if err != nil {
				log.Printf("Insert director error: %v", err)
				http.Error(w, "Failed to insert director", http.StatusInternalServerError)
				return
			}
		} else {
			log.Printf("Query error: %v", err)
			http.Error(w, "Failed to check director", http.StatusInternalServerError)
			return
		}
	}

	// Now insert the movie with the valid director_id
	query := `
        INSERT INTO movies (title, description, release_year, poster, director_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
    `

	var movieID int
	err = db.QueryRow(query, movie.Title, movie.Description, movie.ReleaseYear, movie.Poster, directorID).Scan(&movieID)
	if err != nil {
		log.Printf("Insert error: %v", err)
		http.Error(w, "Failed to create movie", http.StatusInternalServerError)
		return
	}

	movie.ID = movieID
	movie.Director.ID = directorID // Ensure the director ID is included in the response

	// Respond with the created movie
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(movie)
}

// Get all movies
func getMovies(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	query := `
		SELECT 
			m.id, 
			m.title, 
			m.description, 
			m.release_year, 
			m.poster, 
			d.id as director_id,
			d.first_name, 
			d.last_name 
		FROM movies m
		LEFT JOIN directors d ON m.director_id = d.id
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Query error: %v", err)
		http.Error(w, "Unable to fetch movies", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var movies []Movie
	for rows.Next() {
		var movie Movie
		var director Director
		var directorID sql.NullInt64

		if err := rows.Scan(
			&movie.ID,
			&movie.Title,
			&movie.Description,
			&movie.ReleaseYear,
			&movie.Poster,
			&directorID,
			&director.FirstName,
			&director.LastName,
		); err != nil {
			log.Printf("Scan error: %v", err)
			http.Error(w, "Error scanning movie", http.StatusInternalServerError)
			return
		}

		if directorID.Valid {
			director.ID = int(directorID.Int64)
			movie.Director = &director
		}

		movies = append(movies, movie)
	}

	if err = rows.Err(); err != nil {
		log.Printf("Rows error: %v", err)
		http.Error(w, "Error processing movies", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(movies)
}

func getMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get the movie ID from the URL
	params := mux.Vars(r)
	movieID := params["id"]

	// Query to fetch movie by ID
	query := `
        SELECT 
            m.id, 
            m.title, 
            m.description, 
            m.release_year, 
            m.poster, 
            d.id as director_id,
            d.first_name, 
            d.last_name 
        FROM movies m
        LEFT JOIN directors d ON m.director_id = d.id
        WHERE m.id = $1
    `

	// Execute the query
	row := db.QueryRow(query, movieID)

	// Create variables to store the movie data
	var movie Movie
	var director Director
	var directorID sql.NullInt64

	// Scan the result into variables
	err := row.Scan(
		&movie.ID,
		&movie.Title,
		&movie.Description,
		&movie.ReleaseYear,
		&movie.Poster,
		&directorID,
		&director.FirstName,
		&director.LastName,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Movie not found", http.StatusNotFound)
		} else {
			log.Printf("Query error: %v", err)
			http.Error(w, "Error fetching movie", http.StatusInternalServerError)
		}
		return
	}

	// If a director is found, set the director details
	if directorID.Valid {
		director.ID = int(directorID.Int64)
		movie.Director = &director
	}

	// Respond with the movie details
	json.NewEncoder(w).Encode(movie)
}

func deleteMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get movie ID from URL params
	params := mux.Vars(r)
	movieId := params["id"]

	// Execute the delete query
	query := `DELETE FROM movies WHERE id = $1`

	// Delete the movie by ID
	_, err := db.Exec(query, movieId)
	if err != nil {
		log.Printf("Error deleting movie: %v", err)
		http.Error(w, "Failed to delete movie", http.StatusInternalServerError)
		return
	}

	// Respond with a success message
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Movie with ID %s successfully deleted", movieId),
	})
}
func updateMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get movie ID from URL params
	params := mux.Vars(r)
	movieId := params["id"]

	// Decode the request body to get the updated movie details
	var movie Movie
	err := json.NewDecoder(r.Body).Decode(&movie)
	if err != nil {
		log.Printf("Decode error: %v", err)
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Check if the director exists, if not, insert the director
	var directorID int
	err = db.QueryRow(`
        SELECT id FROM directors WHERE first_name = $1 AND last_name = $2
    `, movie.Director.FirstName, movie.Director.LastName).Scan(&directorID)

	// If the director doesn't exist, insert the director and get the ID
	if err != nil {
		if err == sql.ErrNoRows {
			err = db.QueryRow(`
                INSERT INTO directors (first_name, last_name)
                VALUES ($1, $2)
                RETURNING id
            `, movie.Director.FirstName, movie.Director.LastName).Scan(&directorID)
			if err != nil {
				log.Printf("Insert director error: %v", err)
				http.Error(w, "Failed to insert director", http.StatusInternalServerError)
				return
			}
		} else {
			log.Printf("Query error: %v", err)
			http.Error(w, "Failed to check director", http.StatusInternalServerError)
			return
		}
	}

	// Now update the movie with the new details and valid director_id
	query := `
        UPDATE movies
        SET title = $1, description = $2, release_year = $3, poster = $4, director_id = $5
        WHERE id = $6
    `

	// Execute the update query
	_, err = db.Exec(query, movie.Title, movie.Description, movie.ReleaseYear, movie.Poster, directorID, movieId)
	if err != nil {
		log.Printf("Update error: %v", err)
		http.Error(w, "Failed to update movie", http.StatusInternalServerError)
		return
	}

	// Respond with a success message
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Movie with ID %s successfully updated", movieId),
	})
}

func main() {
	connectToDB()
	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/movies", getMovies).Methods("GET")
	r.HandleFunc("/movies", createMovie).Methods("POST")
	r.HandleFunc("/movies/{id}", getMovie).Methods("GET")
	r.HandleFunc("/movies/{id}", deleteMovie).Methods("DELETE")
	r.HandleFunc("/movies/{id}", updateMovie).Methods("PUT")

	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
