use std::fmt::Display;

pub fn print_vec<'a, T>(x: Box<dyn Iterator<Item = T> + 'a>)
where
    T: Display,
{
    println!("[");
    for (i, a) in x.enumerate() {
        if i == 0 {
            print!("    {}", a);
        } else {
            print!(",\n    {}", a);
        }
    }
    println!("\n]");
}
