import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import ThiTruong from './thi-truong/Dashboard';
import QuyMo from './quy-mo/Dashboard';
import SoSanh from './so-sanh/Dashboard';
import Ratios from './ratios/Dashboard';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<ThiTruong />} />
        <Route path="/thi-truong" element={<ThiTruong />} />
        <Route path="/quy-mo" element={<QuyMo />} />
        <Route path="/so-sanh" element={<SoSanh />} />
        <Route path="/ratios" element={<Ratios />} />
      </Routes>
    </Router>
  );
}

export default App;
